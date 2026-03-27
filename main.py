import os
import time
import asyncio
import traceback
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Tuple

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

TRADE_AMOUNT_USD    = float(os.getenv("TRADE_AMOUNT_USD", 5.0))  # fixed $ spend per trade
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", 60))
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE      = int(os.getenv("SIGNATURE_TYPE", 2))
TAKE_PROFIT_PRICE   = 0.99

# Minimum best_ask required to place a trade (avoid thin/empty orderbooks)
MIN_ASK_PRICE = 0.05

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

    @staticmethod
    def parse_clob_token_ids(market: Dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse clobTokenIds from a Gamma API market object.

        IMPORTANT: The Gamma API returns clobTokenIds as a JSON-encoded STRING,
        not a Python list. e.g.:  "clobTokenIds": "[\"123\", \"456\"]"
        All previous code only handled list form and silently got nothing.

        Outcome order is always:  index 0 = YES/UP,  index 1 = NO/DOWN
        as confirmed by the Polymarket docs and live data.
        """
        import json as _json

        clob = market.get("clobTokenIds")

        # Handle JSON string form (the actual API response format)
        if isinstance(clob, str):
            try:
                clob = _json.loads(clob)
            except Exception:
                clob = None

        if isinstance(clob, list) and len(clob) >= 2:
            return str(clob[0]), str(clob[1])

        # Fallback: tokens array (older market format)
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

    # Keep old name as alias so rest of code works unchanged
    def extract_tokens(self, market: Dict) -> Tuple[Optional[str], Optional[str]]:
        return self.parse_clob_token_ids(market)

    def find_current_market(self, window: str) -> Optional[Dict]:
        """
        Confirmed correct approach from live Polymarket URL and official docs:

          - The URL slug (e.g. /event/btc-updown-5m-1774580100) is an EVENT slug.
          - Correct API:  GET /events?slug=<event_slug>   (query param, not path)
          - The event response contains a nested "markets" list.
          - Each market has clobTokenIds as a JSON STRING "[\"id0\",\"id1\"]"
          - index 0 = YES/UP token,  index 1 = NO/DOWN token

        Strategy:
          1. Compute current window ts = (now // interval) * interval  (UTC floor)
          2. Try current, +1, +2 windows (Polymarket pre-creates ~24h ahead),
             and -1 (previous window sometimes still has open orders)
          3. Fallback: list /events?active=true filtered by slug prefix
        """
        import datetime as dt_mod
        interval_sec = MINUTES_MAP[window] * 60
        now = int(time.time())
        current_ts = (now // interval_sec) * interval_sec
        slug_prefix = f"btc-updown-{window}-"

        print(f"[find_market] Searching BTC/{window} | window_ts={current_ts} "
              f"({dt_mod.datetime.fromtimestamp(current_ts, tz=dt_mod.timezone.utc).strftime('%H:%M UTC')})")

        # ── Stage 1: direct event slug lookup ────────────────────────────────
        # Try current window + next 2 (pre-created) + previous (in case of drift)
        for ts_offset in [0, interval_sec, 2 * interval_sec, -interval_sec]:
            ts = current_ts + ts_offset
            slug = f"{slug_prefix}{ts}"
            result = self._fetch_event_by_slug(slug, ts, window)
            if result:
                print(f"[find_market] ✅ Found via /events?slug={slug}")
                print(f"   {result['question'][:140]}")
                return result

        # ── Stage 2: list active events and filter by slug prefix ─────────────
        # Reliable fallback — Polymarket always has 6-7 pre-created windows active
        print(f"[find_market] Direct slugs missed — scanning active events ...")
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/events",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": 50,
                    "order": "startDate",
                    "ascending": "true",
                },
                timeout=15
            )
            if resp.status_code == 200:
                events = resp.json()
                if isinstance(events, list):
                    # Sort by how close the event start is to now
                    for event in events:
                        event_slug = event.get("slug", "")
                        if not event_slug.startswith(slug_prefix):
                            continue
                        # Extract ts from slug for slot tracking
                        ts = self._ts_from_slug(event_slug, interval_sec) or current_ts
                        for m in event.get("markets", []):
                            yes_t, no_t = self.parse_clob_token_ids(m)
                            if yes_t and no_t:
                                print(f"[find_market] ✅ Found via event list: {event_slug}")
                                print(f"   {m.get('question', event.get('title',''))[:140]}")
                                return {
                                    "yes_token_id": yes_t,
                                    "no_token_id": no_t,
                                    "question": m.get("question") or event.get("title", ""),
                                    "slot_ts": ts,
                                    "end_date": m.get("endDate") or event.get("endDate"),
                                }
        except Exception as e:
            print(f"[find_market] Event list scan error: {e}")

        print(f"[find_market] ❌ No BTC/{window} market found. Will retry next loop.")
        return None

    def _fetch_event_by_slug(self, slug: str, ts: int, window: str) -> Optional[Dict]:
        """
        GET /events?slug=<slug>  — query param lookup for an event by its slug.
        The event wraps one or more markets; we extract the first valid one.
        """
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": slug},
                timeout=8
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None
            events = data if isinstance(data, list) else [data]
            for event in events:
                # Verify this is actually the slug we asked for
                if event.get("slug", "") != slug:
                    continue
                for m in event.get("markets", []):
                    yes_t, no_t = self.parse_clob_token_ids(m)
                    if yes_t and no_t:
                        return {
                            "yes_token_id": yes_t,
                            "no_token_id": no_t,
                            "question": m.get("question") or event.get("title", ""),
                            "slot_ts": ts,
                            "end_date": m.get("endDate") or event.get("endDate"),
                        }
        except Exception:
            pass
        return None

    @staticmethod
    def _ts_from_slug(slug: str, interval_sec: int) -> Optional[int]:
        """Extract unix timestamp from slug tail: btc-updown-15m-1774657800 → 1774657800"""
        try:
            ts = int(slug.rsplit("-", 1)[-1])
            if ts % interval_sec == 0 and abs(ts - int(time.time())) < 86400 * 2:
                return ts
        except (ValueError, IndexError):
            pass
        return None

    def _parse_slot_ts(self, market: Dict, window: str) -> int:
        """Derive slot_ts from market endDate: slot_ts = end_ts - interval"""
        import datetime as dt_mod
        end_date_str = market.get("endDate") or market.get("end_date_iso")
        if end_date_str:
            try:
                d = dt_mod.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                end_ts = int(d.timestamp())
                interval_sec = MINUTES_MAP[window] * 60
                return end_ts - interval_sec
            except Exception:
                pass
        return int(time.time())

    def best_ask(self, token_id: str, label: str = "") -> Optional[float]:
        """
        Get the best ask price for a token.
        Tries two methods:
          1. CLOB get_price  (fast, single value)
          2. CLOB /book endpoint  (full orderbook — works even when get_price returns 404)
        """
        if not token_id:
            return None

        tag = f"[{label}] " if label else ""

        # ── Method 1: get_price ───────────────────────────────────────────────
        try:
            price = self.client.get_price(token_id, side="BUY")
            if isinstance(price, dict):
                p = float(price.get("price") or price.get("value") or 0)
            else:
                p = float(price)
            if p > 0:
                print(f"[best_ask] {tag}token={token_id[:16]}... ask={p:.4f} (via get_price)")
                return p
        except Exception as e:
            print(f"[best_ask] {tag}get_price failed: {e} — trying /book ...")

        # ── Method 2: raw CLOB /book endpoint ────────────────────────────────
        try:
            r = requests.get(
                f"{self.host}/book",
                params={"token_id": token_id},
                timeout=8
            )
            if r.status_code == 200:
                book = r.json()
                asks = book.get("asks", [])
                if asks:
                    # asks are sorted best (lowest) first
                    best = float(asks[0].get("price", 0))
                    if best > 0:
                        print(f"[best_ask] {tag}token={token_id[:16]}... ask={best:.4f} (via /book, {len(asks)} levels)")
                        return best
                print(f"[best_ask] {tag}token={token_id[:16]}... /book returned empty asks")
            else:
                print(f"[best_ask] {tag}token={token_id[:16]}... /book HTTP {r.status_code}")
        except Exception as e:
            print(f"[best_ask] {tag}/book failed: {e}")

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


# ── Candle helpers — real Binance OHLCV klines ───────────────────────────────
#
# Binance kline format (each element of the returned list):
#   [0]  open_time  (ms)
#   [1]  open
#   [2]  high
#   [3]  low
#   [4]  close       ← we use this
#   [5]  volume
#   ...
#
# We always request limit=3 so we get:
#   klines[-3]  two candles ago   (fully closed)
#   klines[-2]  previous candle   (fully closed)  ← candle[-2]
#   klines[-1]  current live candle (still open)  ← ignored for signal
#
# Signal logic (momentum follow):
#   candle[-1].close > candle[-2].close  →  UP
#   candle[-1].close < candle[-2].close  →  DOWN
#
# "candle[-1]" here means the most recently CLOSED candle, i.e. klines[-2]
# because klines[-1] is the still-open current candle.

BINANCE_INTERVAL = {"5m": "5m", "15m": "15m"}

# Bybit as fallback (same kline format)
BYBIT_INTERVAL = {"5m": "5", "15m": "15"}


def get_completed_candles(window: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch the last two FULLY CLOSED BTC/USDT candles for the given window
    directly from Binance OHLCV klines API.

    Returns (candle_minus_1_close, candle_minus_2_close) where:
      candle_minus_1 = most recently closed candle
      candle_minus_2 = the one before it

    Signal:
      candle_minus_1 > candle_minus_2  →  caller should BUY UP
      candle_minus_1 < candle_minus_2  →  caller should BUY DOWN

    Falls back to Bybit if Binance is unreachable.
    """
    # ── Primary: Binance ──────────────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": "BTCUSDT",
                "interval": BINANCE_INTERVAL[window],
                "limit": 3,          # [-3]=two ago, [-2]=last closed, [-1]=live
            },
            timeout=8,
        )
        if r.status_code == 200:
            klines = r.json()
            if len(klines) >= 3:
                # klines[-1] is the still-open candle — skip it
                c1_close = float(klines[-2][4])   # most recently closed
                c2_close = float(klines[-3][4])   # one before that
                c1_open_ms = klines[-2][0]
                print(f"[candles/{window}] Binance OHLCV | "
                      f"candle[-1] close={c1_close:.2f}  "
                      f"candle[-2] close={c2_close:.2f}  "
                      f"(opened {_ms_to_hhmm(c1_open_ms)} UTC)")
                return c1_close, c2_close
    except Exception as e:
        print(f"[candles/{window}] Binance error: {e} — trying Bybit ...")

    # ── Fallback: Bybit ───────────────────────────────────────────────────────
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={
                "category": "spot",
                "symbol": "BTCUSDT",
                "interval": BYBIT_INTERVAL[window],
                "limit": 3,
            },
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            # Bybit returns newest first: list[0] = current live, list[1] = last closed
            klines = data.get("result", {}).get("list", [])
            if len(klines) >= 3:
                # klines[0] = live (skip), klines[1] = last closed, klines[2] = one before
                # Bybit format: [startTime, open, high, low, close, volume, turnover]
                c1_close = float(klines[1][4])
                c2_close = float(klines[2][4])
                print(f"[candles/{window}] Bybit OHLCV (fallback) | "
                      f"candle[-1] close={c1_close:.2f}  "
                      f"candle[-2] close={c2_close:.2f}")
                return c1_close, c2_close
    except Exception as e:
        print(f"[candles/{window}] Bybit error: {e}")

    print(f"[candles/{window}] ❌ Could not fetch candles from any source.")
    return None, None


def _ms_to_hhmm(ms: int) -> str:
    """Convert millisecond timestamp to HH:MM string for log readability."""
    import datetime as _dt
    return _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc).strftime("%H:%M")


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


_window_lock: Dict[str, bool] = {"5m": False, "15m": False}  # prevents re-entry during wait


async def check_window(client: PolymarketClient, window: str):
    trade_key = f"BTC-{window}"

    # Guard 1: already have an active trade in this window
    if trade_key in active_trades and int(time.time()) < active_trades[trade_key].expires_at:
        return

    # Guard 2: this window is already mid-execution (signal found, waiting to enter)
    # Without this, a second loop tick during asyncio.sleep(wait_sec) would fire a second trade
    if _window_lock[window]:
        print(f"[{window}] Already processing — skipping duplicate trigger.")
        return

    _window_lock[window] = True
    try:
        await _run_window(client, window, trade_key)
    finally:
        _window_lock[window] = False


async def _run_window(client: PolymarketClient, window: str, trade_key: str):

    market = client.find_current_market(window)
    if not market:
        return

    # ── Real OHLCV candle signal ──────────────────────────────────────────────
    # c1 = most recently CLOSED candle close price
    # c2 = the candle before that close price
    # Momentum follow: c1 > c2 → UP,  c1 < c2 → DOWN
    c1, c2 = get_completed_candles(window)
    if c1 is None or c2 is None:
        print(f"[SIGNAL/{window}] Candle data unavailable — skipping.")
        return
    if c1 == c2:
        print(f"[SIGNAL/{window}] Candles equal ({c1:.2f}) — no signal, skipping.")
        return

    signal = "UP" if c1 > c2 else "DOWN"
    pct_move = abs(c1 - c2) / c2 * 100
    print(f"[SIGNAL/{window}] {'⬆' if signal == 'UP' else '⬇'} {signal} | "
          f"c1={c1:.2f}  c2={c2:.2f}  Δ={pct_move:.3f}%")

    wait_sec = WAIT_SECONDS[window]
    print(f"[SIGNAL/{window}] Waiting {wait_sec}s into live candle before entry ...")
    await asyncio.sleep(wait_sec)

    # Re-fetch market after the wait — window may have rolled
    market = client.find_current_market(window)
    if not market:
        print(f"[SIGNAL/{window}] Market gone after wait — skipping.")
        return

    token_id = market["yes_token_id"] if signal == "UP" else market["no_token_id"]
    direction = f"BTC {signal} [{window}]"

    # Log both token IDs so we can verify which one is UP vs DOWN
    print(f"[tokens/{window}] YES/UP  token: {market['yes_token_id'][:24]}...")
    print(f"[tokens/{window}] NO/DOWN token: {market['no_token_id'][:24]}...")
    print(f"[tokens/{window}] Using {signal} token: {token_id[:24]}...")

    ask = client.best_ask(token_id, label=signal)
    if ask is None:
        # Check the opposite token to diagnose whether it's a token-order issue
        other_id = market["no_token_id"] if signal == "UP" else market["yes_token_id"]
        other_ask = client.best_ask(other_id, label=f"OTHER({'DOWN' if signal=='UP' else 'UP'})")
        if other_ask is not None:
            print(f"[WARN] {signal} token has no orderbook but the opposite token does "
                  f"(ask={other_ask:.4f}). Token index 0/1 mapping may be inverted for this market.")
        print(f"[SKIP] No orderbook for {direction} — aborting.")
        return
    if ask < MIN_ASK_PRICE:
        print(f"[SKIP] Ask {ask:.4f} too low for {direction} — stale market.")
        return

    # Fixed dollar amount: shares = $TRADE_AMOUNT_USD / ask price
    # Round down to 2 decimal places to avoid over-spending
    shares = round(TRADE_AMOUNT_USD / ask, 2)
    cost = shares * ask
    print(f"[ENTRY] {direction} | ask={ask:.4f} | shares={shares} | cost≈${cost:.2f}")

    if client.buy(token_id, ask, shares, direction):
        expires_at = _window_expires_at(market, window)
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=market.get("slot_ts", int(time.time())),
            expires_at=expires_at,
            token_id=token_id,
            side=signal,
        )
        client.place_take_profit(token_id, shares, f"{direction} TP")
        print(f"[TRADE PLACED] {direction} | shares={shares} | cost≈${cost:.2f} | TP @ {TAKE_PROFIT_PRICE}")


async def run_async():
    print("[bot] BTC 5m/15m Candle Momentum Bot")
    print(f"[bot] Dry-run={DRY_RUN} | Trade=${TRADE_AMOUNT_USD} fixed | TP={TAKE_PROFIT_PRICE} | Poll={POLL_INTERVAL_SEC}s")
    client = PolymarketClient()

    while True:
        try:
            sweep_claims()
            print(f"[loop {time.strftime('%H:%M:%S')}] Checking BTC 5m & 15m ...")
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
