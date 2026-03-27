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

@dataclass
class TradeRecord:
    window: str
    slot_ts: int
    expires_at: int
    token_id: str
    side: str             # "UP" or "DOWN"
    claimed: bool = False
    tp_placed: bool = False

WINDOWS = ["5m", "15m"]
WAIT_SECONDS = {"5m": 30, "15m": 360}
MINUTES_MAP = {"5m": 5, "15m": 15}
CANDLE_INTERVALS = {"5m": 300, "15m": 900}

SHARES_PER_TRADE    = float(os.getenv("SHARES_PER_TRADE", 5.0))
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", 60))
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE      = int(os.getenv("SIGNATURE_TYPE", 2))
TAKE_PROFIT_PRICE   = 0.99

price_history: Dict[str, deque] = {w: deque(maxlen=100) for w in WINDOWS}
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

        print(f"[bot] BTC Candle Momentum Bot started | Dry-run: {DRY_RUN} | TP @ {TAKE_PROFIT_PRICE}")

    def get_btc_price(self) -> Optional[float]:
        """Get BTC price from Binance (fast & reliable)"""
        try:
            resp = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=8)
            if resp.status_code == 200:
                return float(resp.json()["price"])
        except Exception as e:
            print(f"[Binance price error] {e}")

        # Fallback to CoinGecko
        try:
            resp = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", timeout=8)
            if resp.status_code == 200:
                return float(resp.json()["bitcoin"]["usd"])
        except Exception:
            pass

        print("[price] Failed to get BTC price from all sources")
        return None

    # === Rest of the methods (extract_tokens, find_current_market, best_ask, place_market_buy, place_take_profit) ===
    # Keep them exactly as in the previous version I sent you

    def extract_tokens(self, market: dict) -> tuple[Optional[str], Optional[str]]:
        clob = market.get("clobTokenIds")
        if isinstance(clob, list) and len(clob) >= 2:
            return str(clob[0]), str(clob[1])   # YES=UP, NO=DOWN
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
                    return {"yes_token": yes_t, "no_token": no_t, "slot_ts": ts}
        return None

    def _fetch_by_slug(self, slug: str) -> Optional[dict]:
        for base in ["https://gamma-api.polymarket.com/markets", "https://gamma-api.polymarket.com/events"]:
            try:
                r = requests.get(f"{base}?slug={slug}", timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    return data[0] if isinstance(data, list) and data else None
            except:
                continue
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        try:
            p = self.client.get_price(token_id, side="BUY")
            if isinstance(p, dict):
                return float(p.get("price") or p.get("value") or 0)
            return float(p)
        except:
            return None

    def place_market_buy(self, token_id: str, shares: float, comment: str) -> bool:
        if DRY_RUN:
            print(f"[DRY RUN] Market BUY → {comment} x {shares}")
            return True
        print(f"[MARKET BUY] {comment}")
        try:
            mo = MarketOrderArgs(token_id=token_id, amount=shares, side=BUY, order_type=OrderType.FOK)
            signed = self.client.create_market_order(mo)
            resp = self.client.post_order(signed, OrderType.FOK)
            print(f"[BUY SUCCESS] {resp}")
            return True
        except Exception as e:
            print(f"[BUY FAILED] {e}")
            return False

    def place_take_profit(self, token_id: str, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] TP SELL @ {TAKE_PROFIT_PRICE} → {comment}")
            return
        print(f"[TP] Placing SELL limit @ {TAKE_PROFIT_PRICE} for {comment}")
        try:
            order = OrderArgs(token_id=token_id, price=TAKE_PROFIT_PRICE, size=shares, side=SELL)
            signed = self.client.create_order(order)
            resp = self.client.post_order(signed, OrderType.GTC)
            print(f"[TP PLACED] {resp}")
        except Exception as e:
            print(f"[TP FAILED] {e}")


# === Price History & Candle Logic ===
def update_price_history(client: PolymarketClient):
    price = client.get_btc_price()
    if price:
        now = int(time.time())
        for w in WINDOWS:
            price_history[w].append((now, price))

def get_last_two_candles(window: str) -> Tuple[Optional[float], Optional[float]]:
    data = list(price_history[window])
    if len(data) < 8:
        return None, None

    interval = CANDLE_INTERVALS[window]
    now = int(time.time())
    current_start = (now // interval) * interval

    closes = []
    last_candle_start = None
    for ts, p in reversed(data):
        candle_start = (ts // interval) * interval
        if candle_start < current_start:
            if not closes or candle_start != last_candle_start:
                closes.append(p)
                last_candle_start = candle_start
            if len(closes) >= 2:
                break

    return (closes[0], closes[1]) if len(closes) >= 2 else (None, None)


# === Main functions (sweep_claims, check_window, run) remain the same as previous version ===

def sweep_claims():
    now = int(time.time())
    for key, rec in list(active_trades.items()):
        if rec.claimed or now < rec.expires_at:
            continue
        print(f"[sweep] {key} | Window expired — please claim manually on Polymarket")
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
    if last_close > prev_close + 0.0001:      # small filter to avoid noise
        signal = "UP"
    elif last_close < prev_close - 0.0001:
        signal = "DOWN"

    if not signal:
        return

    wait_sec = WAIT_SECONDS[window]
    print(f"[SIGNAL/{window}] {signal} detected (last {last_close:.2f} vs prev {prev_close:.2f}) — waiting {wait_sec}s...")
    time.sleep(wait_sec)

    market_data = client.find_current_market(window)
    if not market_data:
        return

    token_id = market_data["yes_token"] if signal == "UP" else market_data["no_token"]
    direction = f"BTC {signal}"

    if client.place_market_buy(token_id, SHARES_PER_TRADE, direction):
        expires_at = market_data.get("slot_ts", int(time.time())) + MINUTES_MAP[window] * 60
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=int(time.time()),
            expires_at=expires_at,
            token_id=token_id,
            side=signal,
        )
        client.place_take_profit(token_id, SHARES_PER_TRADE, f"{direction} TP")
        print(f"[TRADE PLACED] {direction} on {window} | TP set at 0.99")


def run():
    print("[bot] BTC 5m/15m Candle Bot (Binance price feed)")
    client = PolymarketClient()

    while True:
        try:
            update_price_history(client)
            sweep_claims()
            for window in WINDOWS:
                check_window(client, window)
            time.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            time.sleep(30)


if __name__ == "__main__":
    run()
