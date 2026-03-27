import os
import time
import traceback
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
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

price_history: Dict[str, deque] = {w: deque(maxlen=150) for w in WINDOWS}
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

    def extract_tokens(self, market: Dict) -> tuple[Optional[str], Optional[str]]:
        clob = market.get("clobTokenIds")
        if isinstance(clob, list) and len(clob) >= 2:
            return str(clob[0]), str(clob[1])

        tokens = market.get("tokens", [])
        yes_id = no_id = None
        for t in tokens:
            tid = str(t.get("token_id") or t.get("clobTokenId") or "")
            outcome = str(t.get("outcome", "")).lower()
            if outcome in ["yes", "1", "up"]:
                yes_id = tid
            elif outcome in ["no", "0", "down"]:
                no_id = tid
        return yes_id, no_id

    def find_current_market(self, window: str) -> Optional[Dict]:
        interval_sec = MINUTES_MAP[window] * 60
        now = int(time.time())

        print(f"[find_market] Searching for BTC/{window} market...")

        # Wide timestamp search based on your original logic but much broader
        for offset in range(-14400, 1801, 30):  # every 30s over last \~4 hours + forward
            ts = ((now + offset) // interval_sec) * interval_sec
            slug = f"btc-updown-{window}-{ts}"
            market = self._fetch_by_slug(slug)
            if market:
                yes_t, no_t = self.extract_tokens(market)
                if yes_t and no_t:
                    print(f"[find_market] ✅ FOUND BTC/{window} at ts={ts}")
                    print(f"   Question: {market.get('question', '')[:160]}")
                    return {
                        "yes_token_id": yes_t,
                        "no_token_id": no_t,
                        "question": market.get("question", ""),
                        "slot_ts": ts,
                        "end_date": market.get("endDate") or market.get("end_date_iso"),
                    }

        # Your original search fallback
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active": "true", "closed": "false", "limit": 500},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for m in data:
                        q = m.get("question", "").lower()
                        if "bitcoin up or down" in q and window in q:
                            yes_t, no_t = self.extract_tokens(m)
                            if yes_t and no_t:
                                print(f"[find_market] ✅ FOUND via search: BTC/{window}")
                                print(f"   Question: {m.get('question','')[:160]}")
                                return {
                                    "yes_token_id": yes_t,
                                    "no_token_id": no_t,
                                    "question": m.get("question", ""),
                                    "slot_ts": 0,
                                    "end_date": m.get("endDate") or m.get("end_date_iso"),
                                }
        except Exception as e:
            print(f"[search error] {e}")

        print(f"[find_market] ❌ No active BTC/{window} market found this loop. Retrying...")
        return None

    def _fetch_by_slug(self, slug: str) -> Optional[Dict]:
        for base in ["https://gamma-api.polymarket.com/markets", "https://gamma-api.polymarket.com/events"]:
            try:
                resp = requests.get(f"{base}?slug={slug}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        return data[0] if isinstance(data, list) else data
            except Exception:
                continue
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        try:
            price = self.client.get_price(token_id, side="BUY")
            if isinstance(price, dict):
                return float(price.get("price") or price.get("value") or 0)
            return float(price)
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["404", "no orderbook"]):
                print(f"[best_ask] No orderbook ready for {token_id[:20]}...")
            return None

    def buy(self, token_id: str, price: float, shares: float, comment: str) -> bool:
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
        print(f"[TP] Placing limit sell @ {TAKE_PROFIT_PRICE} for {comment}")
        try:
            order = OrderArgs(token_id=token_id, price=TAKE_PROFIT_PRICE, size=shares, side=SELL)
            signed = self.client.create_order(order)
            resp = self.client.post_order(signed, OrderType.GTC)
            print(f"[TP PLACED] {resp}")
        except Exception as e:
            print(f"[TP FAILED] {e}")


# ── Price & Candle helpers (your requested logic) ─────────────────────────────
def get_btc_price() -> Optional[float]:
    for url in [
        "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    ]:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return float(r.json()["price"]) if "binance" in url else float(r.json()["bitcoin"]["usd"])
        except:
            continue
    return None

def update_price_history():
    price = get_btc_price()
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
    last_start = None
    for ts, p in reversed(data):
        c_start = (ts // interval) * interval
        if c_start < current_start:
            if not closes or c_start != last_start:
                closes.append(p)
                last_start = c_start
            if len(closes) >= 2:
                break
    return (closes[0], closes[1]) if len(closes) >= 2 else (None, None)

def _window_expires_at(market: Dict, window: str) -> int:
    slot_ts = market.get("slot_ts", 0)
    interval_sec = MINUTES_MAP.get(window, 300) * 60
    return slot_ts + interval_sec if slot_ts else int(time.time()) + interval_sec

def sweep_claims():
    now = int(time.time())
    for key, record in list(active_trades.items()):
        if record.claimed or now < record.expires_at:
            continue
        print(f"[sweep] {key} expired — please claim manually on Polymarket Portfolio → History")
        record.claimed = True
        del active_trades[key]

def check_window(client: PolymarketClient, window: str):
    trade_key = f"BTC-{window}"
    if trade_key in active_trades and int(time.time()) < active_trades[trade_key].expires_at:
        return

    market = client.find_current_market(window)
    if not market:
        return

    last_close, prev_close = get_last_two_candles(window)
    if not last_close or not prev_close:
        return

    signal = "UP" if last_close > prev_close else "DOWN" if last_close < prev_close else None
    if not signal:
        return

    wait_sec = WAIT_SECONDS[window]
    print(f"[SIGNAL/{window}] BTC {signal} detected — waiting {wait_sec}s into live candle...")
    time.sleep(wait_sec)

    market = client.find_current_market(window)
    if not market:
        return

    token_id = market["yes_token_id"] if signal == "UP" else market["no_token_id"]
    direction = f"BTC {signal} [{window}]"

    if client.buy(token_id, client.best_ask(token_id) or 0.99, SHARES_PER_TRADE, direction):
        expires_at = _window_expires_at(market, window)
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=market.get("slot_ts", int(time.time())),
            expires_at=expires_at,
            token_id=token_id,
            side=signal,
        )
        client.place_take_profit(token_id, SHARES_PER_TRADE, f"{direction} TP")
        print(f"[TRADE PLACED] {direction} | TP set at 0.99")


def run():
    print("[bot] BTC 5m/15m Candle Momentum Bot (using your original market finder + wide search)")
    client = PolymarketClient()

    while True:
        try:
            sweep_claims()
            update_price_history()

            print(f"[loop {time.strftime('%H:%M:%S')}] Checking BTC 5m & 15m...")

            for window in WINDOWS:
                check_window(client, window)

            time.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            time.sleep(30)


if __name__ == "__main__":
    run()
