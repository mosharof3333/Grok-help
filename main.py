import os
import time
import asyncio
import traceback
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
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

# Minimum best_ask required to place a trade (avoid thin/empty orderbooks)
MIN_ASK_PRICE = 0.05

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

    def extract_tokens(self, market: Dict) -> Tuple[Optional[str], Optional[str]]:
        # Try clobTokenIds list first
        clob = market.get("clobTokenIds")
        if isinstance(clob, list) and len(clob) >= 2:
            return str(clob[0]), str(clob[1])

        # Fall back to tokens list
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
        """
        Two-stage market discovery:

        STAGE 1 — Deterministic slug lookup (fast, O(1) API calls):
          Real Polymarket slugs are: btc-updown-5m-{ts} / btc-updown-15m-{ts}
          where ts = (now // interval_sec) * interval_sec  (floor to window boundary).
          We try current window, the one just before, and the next one (in case of
          slight clock drift or a market that opened a few seconds early).
          The correct Gamma API path for slug lookup is:
            GET /markets/slug/{slug}   ← path parameter, NOT ?slug= query param
          Using ?slug= silently ignores the filter and returns unrelated markets.

        STAGE 2 — Slug-prefix search fallback (catches edge cases):
          Query GET /markets?slug=btc-updown-{window}-  (prefix match).
          This is more reliable than keyword-scanning thousands of markets and
          avoids pagination issues with large result sets.
        """
        interval_sec = MINUTES_MAP[window] * 60
        now = int(time.time())
        current_window_ts = (now // interval_sec) * interval_sec

        print(f"[find_market] Searching for BTC/{window} market (current window ts={current_window_ts})...")

        # ── Stage 1: try current window, previous, and next ──────────────────
        for ts in [current_window_ts, current_window_ts - interval_sec, current_window_ts + interval_sec]:
            slug = f"btc-updown-{window}-{ts}"
            market = self._fetch_by_slug_path(slug)
            if market:
                yes_t, no_t = self.extract_tokens(market)
                if yes_t and no_t:
                    print(f"[find_market] ✅ FOUND via slug lookup: {slug}")
                    print(f"   Question: {market.get('question', '')[:160]}")
                    return {
                        "yes_token_id": yes_t,
                        "no_token_id": no_t,
                        "question": market.get("question", ""),
                        "slot_ts": ts,
                        "end_date": market.get("endDate") or market.get("end_date_iso"),
                    }

        # ── Stage 2: prefix search as fallback ───────────────────────────────
        slug_prefix = f"btc-updown-{window}-"
        print(f"[find_market] Slug lookup missed — trying prefix search '{slug_prefix}'...")
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "slug": slug_prefix,   # Gamma supports prefix matching here
                    "active": "true",
                    "closed": "false",
                    "limit": 20,
                    "order": "end_date_iso",
                    "ascending": "false",  # newest first
                },
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for m in data:
                        slug_val = m.get("slug", "")
                        if not slug_val.startswith(slug_prefix):
                            continue  # skip unrelated results
                        yes_t, no_t = self.extract_tokens(m)
                        if yes_t and no_t:
                            print(f"[find_market] ✅ FOUND via prefix search: {slug_val}")
                            print(f"   Question: {m.get('question', '')[:160]}")
                            slot_ts = self._parse_slot_ts(m, window)
                            return {
                                "yes_token_id": yes_t,
                                "no_token_id": no_t,
                                "question": m.get("question", ""),
                                "slot_ts": slot_ts,
                                "end_date": m.get("endDate") or m.get("end_date_iso"),
                            }
        except Exception as e:
            print(f"[find_market] Prefix search error: {e}")

        print(f"[find_market] ❌ No active BTC/{window} market found. Retrying next cycle...")
        return None

    def _fetch_by_slug_path(self, slug: str) -> Optional[Dict]:
        """
        Correct Gamma API slug lookup:
          GET /markets/slug/{slug}   ← path parameter
        The old code used ?slug= as a query param which is silently ignored
        and returns random unrelated markets instead of 404.
        """
        url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return data[0] if isinstance(data, list) else data
        except Exception:
            pass
        return None

    def _parse_slot_ts(self, market: Dict, window: str) -> int:
        """
        FIX #6: Parse real slot_ts from end_date if available,
        rather than returning 0 and causing wrong expiry calculations.
        """
        end_date_str = market.get("endDate") or market.get("end_date_iso")
        if end_date_str:
            try:
                import datetime
                # Handle ISO format: "2025-01-01T12:05:00Z"
                dt = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                end_ts = int(dt.timestamp())
                interval_sec = MINUTES_MAP[window] * 60
                # slot_ts is the start of the window = end_ts - interval
                return end_ts - interval_sec
            except Exception:
                pass
        return int(time.time())  # Fallback: treat now as slot start

    def best_ask(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        try:
            price = self.client.get_price(token_id, side="BUY")
            if isinstance(price, dict):
                p = float(price.get("price") or price.get("value") or 0)
            else:
                p = float(price)
            return p if p > 0 else None
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["404", "no orderbook"]):
                print(f"[best_ask] No orderbook ready for {token_id[:20]}...")
            return None

    def buy(self, token_id: str, price: float, shares: float, comment: str) -> bool:
        if DRY_RUN:
            print(f"[DRY RUN] Market BUY → {comment} x {shares} @ ~{price:.3f}")
            return True
        print(f"[MARKET BUY] {comment}")
        try:
            # FIX #5: MarketOrderArgs does NOT accept order_type — removed it.
            # order_type is only passed to post_order(), not the constructor.
            mo = MarketOrderArgs(token_id=token_id, amount=shares, side=BUY)
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


# ── Price & Candle helpers ────────────────────────────────────────────────────

def get_btc_price() -> Optional[float]:
    for url, parser in [
        (
            "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
            lambda j: float(j["price"])
        ),
        (
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
            lambda j: float(j["bitcoin"]["usd"])
        ),
    ]:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return parser(r.json())
        except Exception:
            continue
    return None


def update_price_history():
    price = get_btc_price()
    if price:
        now = int(time.time())
        for w in WINDOWS:
            price_history[w].append((now, price))


def get_last_two_candles(window: str) -> Tuple[Optional[float], Optional[float]]:
    """
    FIX #8: Added explicit warning when not enough data is accumulated yet,
    so the silent startup blackout is visible to the operator.
    """
    data = list(price_history[window])
    if len(data) < 8:
        remaining = 8 - len(data)
        print(f"[candles/{window}] Warming up — need {remaining} more price samples "
              f"(~{remaining * POLL_INTERVAL_SEC}s)...")
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
    """
    FIX #6: Uses real slot_ts (parsed from end_date) so expiry is accurate.
    Fallback is now + interval rather than an arbitrary guess.
    """
    slot_ts = market.get("slot_ts", 0)
    interval_sec = MINUTES_MAP.get(window, 300) * 60
    if slot_ts and slot_ts > 0:
        return slot_ts + interval_sec
    # Derive from end_date if present
    end_date_str = market.get("end_date")
    if end_date_str:
        try:
            import datetime
            dt = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            pass
    return int(time.time()) + interval_sec


def sweep_claims():
    """
    FIX #7: Collect keys to delete first, then delete — avoids mutating
    the dict while iterating even if list() wrapping is later removed.
    """
    now = int(time.time())
    to_delete = []
    for key, record in active_trades.items():
        if record.claimed or now < record.expires_at:
            continue
        print(f"[sweep] {key} expired — please claim manually on Polymarket Portfolio → History")
        record.claimed = True
        to_delete.append(key)
    for key in to_delete:
        del active_trades[key]


async def wait_without_blocking(seconds: float, label: str):
    """
    FIX #9: Replaces time.sleep() with async sleep so the event loop
    continues processing other windows and price updates during waits.
    """
    print(f"[wait] Sleeping {seconds}s for {label}...")
    await asyncio.sleep(seconds)


async def check_window(client: PolymarketClient, window: str):
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
        print(f"[SIGNAL/{window}] Candles flat — no trade.")
        return

    wait_sec = WAIT_SECONDS[window]
    print(f"[SIGNAL/{window}] BTC {signal} detected — waiting {wait_sec}s into live candle...")

    # FIX #9: Non-blocking wait
    await wait_without_blocking(wait_sec, f"BTC/{window} entry")

    market = client.find_current_market(window)
    if not market:
        return

    token_id = market["yes_token_id"] if signal == "UP" else market["no_token_id"]
    direction = f"BTC {signal} [{window}]"

    # FIX #10: Abort if orderbook is empty or price is suspiciously thin
    ask = client.best_ask(token_id)
    if ask is None:
        print(f"[SKIP] No orderbook for {direction} — aborting trade to avoid bad fill.")
        return
    if ask < MIN_ASK_PRICE:
        print(f"[SKIP] Ask price {ask:.4f} too low (< {MIN_ASK_PRICE}) for {direction} — likely stale market.")
        return

    if client.buy(token_id, ask, SHARES_PER_TRADE, direction):
        expires_at = _window_expires_at(market, window)
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=market.get("slot_ts", int(time.time())),
            expires_at=expires_at,
            token_id=token_id,
            side=signal,
        )
        client.place_take_profit(token_id, SHARES_PER_TRADE, f"{direction} TP")
        print(f"[TRADE PLACED] {direction} | Ask={ask:.4f} | TP set at {TAKE_PROFIT_PRICE}")


async def run_async():
    print("[bot] BTC 5m/15m Candle Momentum Bot — Fixed edition")
    client = PolymarketClient()

    while True:
        try:
            sweep_claims()
            update_price_history()

            print(f"[loop {time.strftime('%H:%M:%S')}] Checking BTC 5m & 15m...")

            # FIX #9: Run both window checks concurrently — neither blocks the other
            await asyncio.gather(
                check_window(client, "5m"),
                check_window(client, "15m"),
            )

            await asyncio.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            await asyncio.sleep(30)


def run():
    asyncio.run(run_async())


if __name__ == "__main__":
    run()
