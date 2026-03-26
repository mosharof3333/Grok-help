import os
import time
import traceback
import requests
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

@dataclass
class Snapshot:
    asset: str
    yes_token: str
    no_token: str
    up_ask: Optional[float]
    down_ask: Optional[float]

@dataclass
class TradeRecord:
    """Tracks one active trade for a given window slot."""
    window: str
    slot_ts: int          # aligned timestamp that identifies the window slot
    expires_at: int       # unix timestamp when the window closes
    yes_token: str
    no_token: str
    side: str             # "BTC_UP" or "ETH_UP"
    claimed: bool = False

WINDOWS = ["5m", "15m", "4h"]
THRESHOLDS = {"5m": 0.10, "15m": 0.15, "4h": 0.20}
MINUTES_MAP = {"5m": 5, "15m": 15, "4h": 240}

def get_env_float(key: str, default: float) -> float:
    return float(os.getenv(key, default))

PRICE_GAP_THRESHOLD = get_env_float("PRICE_GAP_THRESHOLD", 0.20)
SHARES_PER_TRADE    = get_env_float("SHARES_PER_TRADE", 5.0)
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", 60))
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE      = int(os.getenv("SIGNATURE_TYPE", 2))

# ── active trade registry ─────────────────────────────────────────────────────
# key = (asset_pair, window)  e.g. ("BTC/ETH", "5m")
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
        print(f"[bot] Initialized | Sig: {SIGNATURE_TYPE} | Dry-run: {DRY_RUN} | Shares: {SHARES_PER_TRADE}")

    # ── token extraction ───────────────────────────────────────────────────────
    def extract_tokens(self, market: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        clob = market.get("clobTokenIds")
        if clob:
            if isinstance(clob, list) and len(clob) >= 2:
                return str(clob[0]), str(clob[1])
            if isinstance(clob, str):
                try:
                    ids = json.loads(clob.replace("'", '"'))
                    if isinstance(ids, list) and len(ids) >= 2:
                        return str(ids[0]), str(ids[1])
                except Exception:
                    pass

        tokens = market.get("tokens", [])
        if tokens and len(tokens) >= 2:
            yes_id = no_id = None
            for t in tokens:
                tid = str(t.get("token_id") or t.get("clobTokenId") or t.get("id") or "")
                outcome = str(t.get("outcome", "")).lower()
                if outcome in ["yes", "1", "up"]:
                    yes_id = tid
                elif outcome in ["no", "0", "down"]:
                    no_id = tid
            if yes_id and no_id:
                return yes_id, no_id
        return None, None

    # ── market discovery ───────────────────────────────────────────────────────
    def find_current_market(self, asset: str, window: str) -> Optional[Dict]:
        interval_sec = MINUTES_MAP.get(window, 300) * 60
        now = int(time.time())

        for offset in [0, -interval_sec, -300, 300, -600]:
            ts = ((now + offset) // interval_sec) * interval_sec
            slug = f"{asset.lower()}-updown-{window}-{ts}"
            market = self._fetch_by_slug(slug)
            if market:
                yes_t, no_t = self.extract_tokens(market)
                if yes_t and no_t:
                    print(f"[find_market] Found {asset}/{window}: {market.get('question', '')[:100]}")
                    end_date = market.get("endDate") or market.get("end_date_iso")
                    return {
                        "yes_token_id": yes_t,
                        "no_token_id": no_t,
                        "question": market.get("question", ""),
                        "slot_ts": ts,
                        "end_date": end_date,
                    }

        # Search fallback
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active": "true", "closed": "false", "limit": 300},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for m in data:
                        q = m.get("question", "").lower()
                        if f"{asset.lower()} up or down" in q and window in q:
                            yes_t, no_t = self.extract_tokens(m)
                            if yes_t and no_t:
                                print(f"[find_market] Found via search: {m.get('question','')[:100]}")
                                return {
                                    "yes_token_id": yes_t,
                                    "no_token_id": no_t,
                                    "question": m.get("question", ""),
                                    "slot_ts": 0,
                                    "end_date": m.get("endDate") or m.get("end_date_iso"),
                                }
        except Exception as e:
            print(f"[search error] {e}")

        print(f"[find_market] No active {asset.upper()}/{window} market")
        return None

    def _fetch_by_slug(self, slug: str) -> Optional[Dict]:
        for base in ["https://gamma-api.polymarket.com/markets",
                     "https://gamma-api.polymarket.com/events"]:
            try:
                resp = requests.get(f"{base}?slug={slug}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        return data[0] if isinstance(data, list) else data
            except Exception:
                continue
        return None

    # ── price feed ─────────────────────────────────────────────────────────────
    def best_ask(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        token_id = str(token_id).strip()
        try:
            price = self.client.get_price(token_id, side="BUY")
            if price is None:
                return None
            if isinstance(price, dict):
                price_val = price.get("price") or price.get("value")
                if price_val is not None:
                    return float(price_val)
            return float(price)
        except Exception as e:
            err_str = str(e).lower()
            if "404" in err_str or "no orderbook" in err_str or "not found" in err_str:
                print(f"[best_ask] No orderbook ready yet for {token_id[:20]}... (normal)")
            else:
                print(f"[best_ask error] token={token_id[:30]}... | {type(e).__name__}: {str(e)[:120]}")
            return None

    # ── order placement ────────────────────────────────────────────────────────
    def buy(self, token_id: str, price: float, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] Would buy → {comment} @ {price:.3f} x {shares}")
            return
        print(f"[BUY] {comment}")
        try:
            order = OrderArgs(token_id=str(token_id), price=price, size=shares, side=BUY)
            signed = self.client.create_order(order)
            resp   = self.client.post_order(signed, OrderType.GTC)
            print(f"[BUY SUCCESS] {resp}")
        except Exception as e:
            print(f"[BUY FAILED] {e}")

    # ── auto-claim ─────────────────────────────────────────────────────────────
    def claim_winnings(self, token_id: str, comment: str):
        """
        Attempt to redeem a resolved position.

        The py_clob_client exposes `redeem_positions` (or `settle_market`
        depending on SDK version).  We try both and fall back to a raw REST
        call against the CLOB redeem endpoint.
        """
        if DRY_RUN:
            print(f"[DRY RUN] Would claim → {comment}")
            return

        print(f"[CLAIM] Attempting to claim winnings for {comment} | token={token_id[:30]}...")
        claimed = False

        # Attempt 1: SDK redeem_positions
        if hasattr(self.client, "redeem_positions"):
            try:
                resp = self.client.redeem_positions(token_ids=[token_id])
                print(f"[CLAIM SUCCESS] redeem_positions: {resp}")
                claimed = True
            except Exception as e:
                print(f"[CLAIM] redeem_positions failed: {e}")

        # Attempt 2: SDK settle_market
        if not claimed and hasattr(self.client, "settle_market"):
            try:
                resp = self.client.settle_market(token_id=token_id)
                print(f"[CLAIM SUCCESS] settle_market: {resp}")
                claimed = True
            except Exception as e:
                print(f"[CLAIM] settle_market failed: {e}")

        # Attempt 3: Raw REST call to CLOB redeem endpoint
        if not claimed:
            try:
                headers = {}
                if hasattr(self.client, "_get_auth_headers"):
                    headers = self.client._get_auth_headers()
                elif hasattr(self.client, "get_auth_headers"):
                    headers = self.client.get_auth_headers()

                url  = f"{self.host}/redeem"
                body = {"token_id": token_id}
                resp = requests.post(url, json=body, headers=headers, timeout=15)
                if resp.status_code in (200, 201):
                    print(f"[CLAIM SUCCESS] REST redeem: {resp.json()}")
                    claimed = True
                else:
                    print(f"[CLAIM] REST redeem status={resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"[CLAIM] REST redeem failed: {e}")

        if not claimed:
            print(f"[CLAIM] Could not claim {comment} — market may not be resolved yet.")


# ── snapshot helper ────────────────────────────────────────────────────────────
def get_snapshot(client: PolymarketClient, asset: str, window: str) -> Optional[tuple]:
    """Returns (Snapshot, market_dict) or (None, None)."""
    market = client.find_current_market(asset, window)
    if not market:
        return None, None
    up   = client.best_ask(market["yes_token_id"])
    down = client.best_ask(market["no_token_id"])
    print(f"[{asset.upper()}/{window}] up={up} down={down} | {market.get('question','')[:100]}")
    if up is None or down is None:
        return None, None
    snap = Snapshot(
        asset=asset,
        yes_token=market["yes_token_id"],
        no_token=market["no_token_id"],
        up_ask=up,
        down_ask=down,
    )
    return snap, market


def _window_expires_at(market: Dict, window: str) -> int:
    """
    Derive the unix timestamp when this window slot expires.
    Uses endDate from market data when available; otherwise estimates
    from the slot_ts + window duration.
    """
    end_date = market.get("end_date")
    if end_date:
        import datetime
        try:
            # ISO-8601 strings like "2024-01-01T12:05:00Z"
            dt = datetime.datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            pass

    slot_ts      = market.get("slot_ts", 0)
    interval_sec = MINUTES_MAP.get(window, 300) * 60
    return slot_ts + interval_sec if slot_ts else int(time.time()) + interval_sec


# ── per-window auto-claim sweep ────────────────────────────────────────────────
def sweep_claims(client: PolymarketClient):
    """
    Check every recorded trade.  If the window has expired and the trade
    hasn't been claimed yet, attempt to redeem both legs.
    """
    now = int(time.time())
    for key, record in list(active_trades.items()):
        if record.claimed:
            continue
        if now < record.expires_at:
            remaining = record.expires_at - now
            print(f"[sweep] {key} | window still open — {remaining}s remaining")
            continue

        print(f"[sweep] {key} | window EXPIRED — auto-claiming…")
        client.claim_winnings(record.yes_token, f"{key} YES leg")
        client.claim_winnings(record.no_token,  f"{key} NO  leg")
        record.claimed = True
        print(f"[sweep] {key} | claim attempt complete, slot cleared")
        del active_trades[key]   # remove so next window can trade again


# ── main check logic ───────────────────────────────────────────────────────────
def check_window(client: PolymarketClient, window: str):
    trade_key = f"BTC/ETH-{window}"

    # ── guard: only 1 trade allowed per window slot ───────────────────────────
    if trade_key in active_trades:
        rec = active_trades[trade_key]
        now = int(time.time())
        if now < rec.expires_at:
            remaining = rec.expires_at - now
            print(f"[gate/{window}] Trade already placed for this slot "
                  f"({rec.side}) — {remaining}s until window closes. Skipping.")
            return
        # Window expired but sweep hasn't run yet — let sweep handle it
        print(f"[gate/{window}] Previous slot expired, claim sweep will handle it.")
        return

    # ── fetch prices ──────────────────────────────────────────────────────────
    btc, btc_market = get_snapshot(client, "BTC", window)
    eth, eth_market = get_snapshot(client, "ETH", window)
    if not (btc and eth):
        return

    threshold = THRESHOLDS.get(window, PRICE_GAP_THRESHOLD)
    gap       = btc.up_ask - eth.up_ask
    print(f"[gap/{window}] BTC_up={btc.up_ask:.2f} ETH_up={eth.up_ask:.2f} "
          f"diff={gap:+.2f} (thresh ±{threshold})")

    side = None
    if gap <= -threshold:
        print(f"[SIGNAL/{window}] BTC UP cheap → BUY BTC UP + ETH DOWN")
        client.buy(btc.yes_token, btc.up_ask, SHARES_PER_TRADE, f"BTC UP @ {btc.up_ask:.3f} [{window}]")
        client.buy(eth.no_token,  eth.down_ask, SHARES_PER_TRADE, f"ETH DOWN @ {eth.down_ask:.3f} [{window}]")
        side = "BTC_UP"
    elif gap >= threshold:
        print(f"[SIGNAL/{window}] ETH UP cheap → BUY ETH UP + BTC DOWN")
        client.buy(eth.yes_token, eth.up_ask, SHARES_PER_TRADE, f"ETH UP @ {eth.up_ask:.3f} [{window}]")
        client.buy(btc.no_token,  btc.down_ask, SHARES_PER_TRADE, f"BTC DOWN @ {btc.down_ask:.3f} [{window}]")
        side = "ETH_UP"

    if side:
        # Use the BTC market timing as reference (both assets share the same window)
        expires_at = _window_expires_at(btc_market, window)
        slot_ts    = btc_market.get("slot_ts", 0)
        active_trades[trade_key] = TradeRecord(
            window=window,
            slot_ts=slot_ts,
            expires_at=expires_at,
            yes_token=btc.yes_token if side == "BTC_UP" else eth.yes_token,
            no_token =eth.no_token  if side == "BTC_UP" else btc.no_token,
            side=side,
        )
        print(f"[gate/{window}] Trade recorded ({side}). "
              f"Next trade allowed after {time.strftime('%H:%M:%S', time.localtime(expires_at))}.")


# ── main loop ──────────────────────────────────────────────────────────────────
def run():
    print(f"[bot] BTC/ETH Correlation Arbitrage Bot started | Dry-run: {DRY_RUN}")
    client = PolymarketClient()

    while True:
        try:
            # 1) Attempt to claim any expired trades first
            sweep_claims(client)

            # 2) Look for new signals in each window
            for window in WINDOWS:
                check_window(client, window)

            time.sleep(POLL_INTERVAL_SEC)

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            time.sleep(30)


if __name__ == "__main__":
    run()
