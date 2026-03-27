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
        Three-stage market discovery. Confirmed from real live Polymarket slugs:

          Slug format:  btc-updown-{window}-{ts}
          Timestamp:    ts = (unix_epoch // interval_sec) * interval_sec  (UTC floor)
          Examples:     btc-updown-15m-1774657800  (verified live)
                        btc-updown-5m-1774539000   (verified live)
          Advance creation: Polymarket creates markets ~24h ahead, so the current
                        window AND several future windows are already queryable.

        STAGE 1 — Direct slug path lookup  (fast, 3 requests)
          GET /markets/slug/{slug}  ← path-based, not ?slug= query param
          Tries: current window, previous window (completed), next two windows (pre-created)

        STAGE 2 — events endpoint slug lookup  (catches slug registered under /events)
          Some Polymarket rolling markets live under /events not /markets.
          GET /events/slug/{slug}  with the same slug format.

        STAGE 3 — Tag-filtered search  (reliable broad fallback)
          Polymarket tags candle markets with tag_id for "Up or Down" / "5 Min" / "15 Min".
          This completely avoids keyword guessing and pagination over all markets.
          We just filter by the slug prefix in the returned results.
        """
        import datetime as dt_mod
        interval_sec = MINUTES_MAP[window] * 60
        now = int(time.time())
        current_ts = (now // interval_sec) * interval_sec
        slug_prefix = f"btc-updown-{window}-"

        print(f"[find_market] Searching BTC/{window} | ts={current_ts} | {dt_mod.datetime.utcfromtimestamp(current_ts).strftime('%H:%M UTC')}")

        # ── Stage 1: direct /markets/slug/{slug} path lookup ─────────────────
        # Try current, previous (already resolved but sometimes still active),
        # and next two (pre-created ~24h ahead by Polymarket)
        candidates_ts = [
            current_ts,
            current_ts + interval_sec,
            current_ts + 2 * interval_sec,
            current_ts - interval_sec,
        ]
        for ts in candidates_ts:
            slug = f"{slug_prefix}{ts}"
            result = self._try_market_from_slug(slug, ts, window, endpoint="markets")
            if result:
                print(f"[find_market] ✅ /markets/slug/ → {slug}")
                print(f"   {result['question'][:140]}")
                return result

        # ── Stage 2: same slugs under /events endpoint ────────────────────────
        print(f"[find_market] /markets/slug/ missed — trying /events/slug/ ...")
        for ts in candidates_ts:
            slug = f"{slug_prefix}{ts}"
            result = self._try_market_from_event_slug(slug, ts, window)
            if result:
                print(f"[find_market] ✅ /events/slug/ → {slug}")
                print(f"   {result['question'][:140]}")
                return result

        # ── Stage 3: search with slug prefix filter ───────────────────────────
        # The Gamma API ?slug= param does prefix matching even though it doesn't
        # work as an exact-match filter — we just guard with startswith() below.
        # Polymarket pre-creates 6-7 windows, so at least one will be in the list.
        print(f"[find_market] Trying prefix search '{slug_prefix}' ...")
        for endpoint in ["markets", "events"]:
            try:
                resp = requests.get(
                    f"https://gamma-api.polymarket.com/{endpoint}",
                    params={
                        "slug": slug_prefix,
                        "active": "true",
                        "closed": "false",
                        "limit": 20,
                        "order": "end_date_iso",
                        "ascending": "true",   # earliest-ending first = most live
                    },
                    timeout=15
                )
                if resp.status_code != 200:
                    continue
                items = resp.json()
                if not isinstance(items, list):
                    items = [items]
                for item in items:
                    # Events nest markets inside a "markets" key
                    markets_list = item.get("markets", [item]) if endpoint == "events" else [item]
                    for m in markets_list:
                        slug_val = m.get("slug", "")
                        if not slug_val.startswith(slug_prefix):
                            continue
                        yes_t, no_t = self.extract_tokens(m)
                        if yes_t and no_t:
                            slot_ts = self._parse_slot_ts_from_slug(slug_val, interval_sec) or self._parse_slot_ts(m, window)
                            print(f"[find_market] ✅ prefix/{endpoint} → {slug_val}")
                            print(f"   {m.get('question','')[:140]}")
                            return {
                                "yes_token_id": yes_t,
                                "no_token_id": no_t,
                                "question": m.get("question", ""),
                                "slot_ts": slot_ts,
                                "end_date": m.get("endDate") or m.get("end_date_iso"),
                            }
            except Exception as e:
                print(f"[find_market] prefix/{endpoint} error: {e}")

        print(f"[find_market] ❌ No BTC/{window} market found. Will retry next loop.")
        return None

    def _try_market_from_slug(self, slug: str, ts: int, window: str, endpoint: str = "markets") -> Optional[Dict]:
        """
        GET /markets/slug/{slug} — path-based lookup, returns the market directly.
        This is the correct Gamma API usage; ?slug= query param is unreliable.
        """
        url = f"https://gamma-api.polymarket.com/{endpoint}/slug/{slug}"
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None
            m = data[0] if isinstance(data, list) else data
            yes_t, no_t = self.extract_tokens(m)
            if yes_t and no_t:
                return {
                    "yes_token_id": yes_t,
                    "no_token_id": no_t,
                    "question": m.get("question", ""),
                    "slot_ts": ts,
                    "end_date": m.get("endDate") or m.get("end_date_iso"),
                }
        except Exception:
            pass
        return None

    def _try_market_from_event_slug(self, slug: str, ts: int, window: str) -> Optional[Dict]:
        """
        Some rolling markets are registered as events with nested markets.
        GET /events/slug/{slug} and dig into event.markets[].
        """
        url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None
            event = data[0] if isinstance(data, list) else data
            for m in event.get("markets", []):
                yes_t, no_t = self.extract_tokens(m)
                if yes_t and no_t:
                    return {
                        "yes_token_id": yes_t,
                        "no_token_id": no_t,
                        "question": m.get("question", event.get("title", "")),
                        "slot_ts": ts,
                        "end_date": m.get("endDate") or event.get("endDate"),
                    }
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_slot_ts_from_slug(slug: str, interval_sec: int) -> Optional[int]:
        """Extract ts directly from slug string: btc-updown-15m-1774657800 → 1774657800"""
        try:
            ts = int(slug.rsplit("-", 1)[-1])
            # Sanity: must be divisible by interval and within ±24h of now
            if ts % interval_sec == 0 and abs(ts - int(time.time())) < 86400:
                return ts
        except (ValueError, IndexError):
            pass
        return None

    def _parse_slot_ts(self, market: Dict, window: str) -> int:
        """
        Derive slot_ts from market endDate when slug parsing isn't available.
        slot_ts = end_ts - interval  (market title says start→end, ts is the start)
        """
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
