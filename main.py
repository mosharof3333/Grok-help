import os
import time
import traceback
import requests
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from collections import deque

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

# Chainlink
try:
    from web3 import Web3
    HAS_WEB3 = True
except ImportError:
    HAS_WEB3 = False
    print("[WARN] Install web3: pip install web3")

@dataclass
class TradeRecord:
    window: str
    slot_ts: int          # timestamp when the signal candle closed
    expires_at: int
    token_id: str
    side: str             # "UP" or "DOWN"
    claimed: bool = False
    tp_placed: bool = False

WINDOWS = ["5m", "15m"]
WAIT_SECONDS = {"5m": 30, "15m": 360}   # 30s / 6 minutes
MINUTES_MAP = {"5m": 5, "15m": 15}
CANDLE_INTERVALS = {"5m": 300, "15m": 900}  # seconds

SHARES_PER_TRADE    = float(os.getenv("SHARES_PER_TRADE", 5.0))
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", 60))
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE      = int(os.getenv("SIGNATURE_TYPE", 2))
TAKE_PROFIT_PRICE   = 0.99
POLYGON_RPC         = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")

CHAINLINK_BTC_FEED  = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# Price history per window (deque for efficiency)
price_history: Dict[str, deque] = {w: deque(maxlen=50) for w in WINDOWS}  # (timestamp, price)

active_trades: Dict[str, TradeRecord] = {}


class PolymarketClient:
    def __init__(self):
        self.host = "https://clob.polymarket.com"
        private_key = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER")
        if not private_key or not funder:
            raise ValueError("PRIVATE_KEY and FUNDER required")

        self.client = ClobClient(
            host=self.host,
            key=private_key,
            chain_id=137,
            funder=funder,
            signature_type=SIGNATURE_TYPE
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

        self.w3 = Web3(Web3.HTTPProvider(POLYGON_RPC)) if HAS_WEB3 else None
        if self.w3 and self.w3.is_connected():
            print(f"[Chainlink] Connected to Polygon RPC | BTC feed: {CHAINLINK_BTC_FEED[:10]}...")
        else:
            print("[WARN] Chainlink web3 not connected — falling back to live price only (less accurate candles)")

        self.btc_aggregator = None
        if self.w3:
            abi = [
                {"inputs":[],"name":"latestRoundData","outputs":[
                    {"name":"roundId","type":"uint80"},
                    {"name":"answer","type":"int256"},
                    {"name":"startedAt","type":"uint256"},
                    {"name":"updatedAt","type":"uint256"},
                    {"name":"answeredInRound","type":"uint80"}
                ],"stateMutability":"view","type":"function"},
                {"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"}
            ]
            self.btc_aggregator = self.w3.eth.contract(address=CHAINLINK_BTC_FEED, abi=abi)

        print(f"[bot] BTC Candle Momentum Bot | Dry-run: {DRY_RUN} | TP: {TAKE_PROFIT_PRICE}")

    def get_btc_price(self) -> Optional[float]:
        """Get latest BTC price from Chainlink (preferred) or fallback."""
        if self.btc_aggregator:
            try:
                round_data = self.btc_aggregator.functions.latestRoundData().call()
                decimals = self.btc_aggregator.functions.decimals().call()
                price = round_data[1] / (10 ** decimals)
                return float(price)
            except Exception as e:
                print(f"[Chainlink error] {e}")
        # Fallback: you could add Binance API here if needed
        print("[price] Using Chainlink failed — no price")
        return None

    # extract_tokens, find_current_market, _fetch_by_slug, best_ask, place_market_buy, place_take_profit
    # (kept almost identical to previous version — only minor cleanups)

    def extract_tokens(self, market: dict) -> tuple[Optional[str], Optional[str]]:
        clob = market.get("clobTokenIds")
        if isinstance(clob, list) and len(clob) >= 2:
            return str(clob[0]), str(clob[1])  # YES=UP, NO=DOWN
        # fallback omitted for brevity — use your original if needed
        return None, None

    def find_current_market(self, window: str) -> Optional[dict]:
        interval_sec = MINUTES_MAP[window] * 60
        now = int(time.time())
        for offset in [0, -interval_sec]:
            ts = ((now + offset) // interval_sec) * interval_sec
            slug = f"btc-updown-{window}-{ts}"
            market = self._fetch_by_slug(slug)
            if market:
                yes_t, no_t = self.extract_tokens(market)
                if yes_t and no_t:
                    return {
                        "yes_token": yes_t,
                        "no_token": no_t,
                        "slot_ts": ts,
                    }
        return None

    def _fetch_by_slug(self, slug: str) -> Optional[dict]:
        bases = ["https://gamma-api.polymarket.com/markets", "https://gamma-api.polymarket.com/events"]
        for base in bases:
            try:
                r = requests.get(f"{base}?slug={slug}", timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    return data[0] if isinstance(data, list) and data else data
            except:
                continue
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        try:
            p = self.client.get_price(token_id, side="BUY")
            if isinstance(p, dict):
                return float(p.get("price") or p.get("value") or 0)
            return float(p)
        except Exception:
            return None

    def place_market_buy(self, token_id: str, shares: float, comment: str) -> bool:
        if DRY_RUN:
            print(f"[DRY RUN] Market BUY {comment} x {shares}")
            return True
        print(f"[MARKET BUY] {comment}")
        try:
            mo = MarketOrderArgs(token_id=token_id, amount=shares, side=BUY, order_type=OrderType.FOK)
            signed = self.client.create_market_order(mo)
            resp = self.client.post_order(signed, OrderType.FOK)
            print(f"[BUY OK] {resp}")
            return True
        except Exception as e:
            print(f"[BUY FAIL] {e}")
            return False

    def place_take_profit(self, token_id: str, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] TP SELL @ {TAKE_PROFIT_PRICE} {comment}")
            return
        print(f"[TP] Limit SELL @ {TAKE_PROFIT_PRICE} for {comment}")
        try:
            order = OrderArgs(token_id=token_id, price=TAKE_PROFIT_PRICE, size=shares, side=SELL)
            signed = self.client.create_order(order)
            resp = self.client.post_order(signed, OrderType.GTC)
            print(f"[TP PLACED] {resp}")
        except Exception as e:
            print(f"[TP FAIL] {e}")


def update_price_history(client: PolymarketClient):
    price = client.get_btc_price()
    if not price:
        return
    now = int(time.time())
    for window in WINDOWS:
        price_history[window].append((now, price))


def get_last_two_candles(window: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (last_completed_close, previous_close) or (None, None)"""
    data = list(price_history[window])
    if len(data) < 10:  # need enough samples
        return None, None

    interval = CANDLE_INTERVALS[window]
    now = int(time.time())
    current_candle_start = (now // interval) * interval

    # Find closes of previous two completed candles
    closes = []
    for i in range(len(data)-1, -1, -1):
        ts, p = data[i]
        candle_start = (ts // interval) * interval
        if candle_start < current_candle_start and len(closes) < 2:
            # Use last price in that candle as close (approximation)
            if not closes or candle_start != (data[i+1][0] // interval * interval if i+1 < len(data) else 0):
                closes.append(p)
        if len(closes) >= 2:
            break

    if len(closes) >= 2:
        return closes[0], closes[1]  # last completed, one before
    return None, None


def sweep_claims():
    now = int(time.time())
    for key, rec in list(active_trades.items()):
        if rec.claimed or now < rec.expires_at:
            continue
        print(f"[sweep] {key} expired — ready for manual claim in Polymarket Portfolio")
        rec.claimed = True
        del active_trades[key]


def check_window(client: PolymarketClient, window: str):
    trade_key = f"BTC-{window}"
    if trade_key in active_trades and int(time.time()) < active_trades[trade_key].expires_at:
        return

    market_data = client.find_current_market(window)
    if not market_data:
        return

    last_close, prev_close = get_last_two_candles(window)
    if not last_close or not prev_close:
        return

    signal = None
    if last_close > prev_close:
        signal = "UP"
    elif last_close < prev_close:
        signal = "DOWN"

    if not signal:
        return

    wait_sec = WAIT_SECONDS[window]
    print(f"[SIGNAL/{window}] {signal} (last close {last_close:.2f} >/< prev {prev_close:.2f}) — waiting {wait_sec}s...")
    time.sleep(wait_sec)

    # Re-check market
    market_data = client.find_current_market(window)
    if not market_data:
        return

    token_id = market_data["yes_token"] if signal == "UP" else market_data["no_token"]
    direction = f"BTC {signal}"

    success = client.place_market_buy(token_id, SHARES_PER_TRADE, direction)

    if success:
        expires_at = market_data.get("slot_ts", int(time.time())) + MINUTES_MAP[window] * 60
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=int(time.time()),
            expires_at=expires_at,
            token_id=token_id,
            side=signal,
        )
        client.place_take_profit(token_id, SHARES_PER_TRADE, f"{direction} TP")
        active_trades[trade_key].tp_placed = True
        print(f"[TRADE] {direction} placed for {window} | TP @ 0.99")


def run():
    print("[bot] BTC 5m/15m Momentum Bot with Chainlink pricing")
    if not HAS_WEB3:
        print("[WARN] web3 not installed — candle detection will be weak")
    client = PolymarketClient()

    while True:
        try:
            update_price_history(client)
            sweep_claims()

            for window in WINDOWS:
                check_window(client, window)

            time.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            print(f"[ERROR] {e}")
            traceback.print_exc()
            time.sleep(30)


if __name__ == "__main__":
    run()
