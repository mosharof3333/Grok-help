import os
import time
import traceback
import requests
import json
from dataclasses import dataclass
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

WINDOWS = ["5m", "15m", "4h"]
THRESHOLDS = {"5m": 0.10, "15m": 0.15, "4h": 0.20}

def get_env_float(key: str, default: float) -> float:
    return float(os.getenv(key, default))

PRICE_GAP_THRESHOLD = get_env_float("PRICE_GAP_THRESHOLD", 0.20)
SHARES_PER_TRADE = get_env_float("SHARES_PER_TRADE", 5.0)
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", 60))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", 2))

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

    def extract_tokens(self, market: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        """Force string conversion + multiple fallbacks"""
        # clobTokenIds
        clob = market.get("clobTokenIds")
        if clob:
            if isinstance(clob, list) and len(clob) >= 2:
                return str(clob[0]), str(clob[1])
            if isinstance(clob, str):
                try:
                    ids = json.loads(clob.replace("'", '"'))
                    if isinstance(ids, list) and len(ids) >= 2:
                        return str(ids[0]), str(ids[1])
                except:
                    pass

        # tokens array
        tokens = market.get("tokens", [])
        if tokens and len(tokens) >= 2:
            yes_id = None
            no_id = None
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

    def find_current_market(self, asset: str, window: str) -> Optional[Dict]:
        minutes_map = {"5m": 5, "15m": 15, "4h": 240}
        interval_sec = minutes_map.get(window, 300) * 60
        now = int(time.time())

        # Timestamp slug tries
        for offset in [0, -interval_sec, -300, 300, -600]:
            ts = ((now + offset) // interval_sec) * interval_sec
            slug = f"{asset.lower()}-updown-{window}-{ts}"
            market = self._fetch_by_slug(slug)
            if market:
                yes_t, no_t = self.extract_tokens(market)
                if yes_t and no_t:
                    print(f"[find_market] Found via slug: {market.get('question', '')[:100]}")
                    return {"yes_token_id": yes_t, "no_token_id": no_t, "question": market.get("question", ""), "raw": market}
        
        # Search fallback
        print(f"[find_market] Searching active markets for {asset.upper()}/{window}...")
        try:
            resp = requests.get("https://gamma-api.polymarket.com/markets", 
                                params={"active": "true", "closed": "false", "limit": 300}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for m in data:
                        q = m.get("question", "").lower()
                        if f"{asset.lower()} up or down" in q and window in q:
                            yes_t, no_t = self.extract_tokens(m)
                            if yes_t and no_t:
                                print(f"[find_market] Found via search: {m.get('question','')[:100]}")
                                return {"yes_token_id": yes_t, "no_token_id": no_t, "question": m.get("question", ""), "raw": m}
        except Exception as e:
            print(f"[search error] {e}")
        
        print(f"[find_market] No active tradable {asset.upper()}/{window} market")
        return None

    def _fetch_by_slug(self, slug: str) -> Optional[Dict]:
        for base in ["https://gamma-api.polymarket.com/markets", "https://gamma-api.polymarket.com/events"]:
            try:
                resp = requests.get(f"{base}?slug={slug}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        return data[0] if isinstance(data, list) and len(data) > 0 else data
            except:
                continue
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        if not token_id:
            return None
        token_id = str(token_id).strip()  # Force string
        try:
            price = self.client.get_price(token_id, side="BUY")
            if price is not None:
                return float(price)
            return None
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "No orderbook" in err_str:
                print(f"[best_ask] No orderbook yet for token {token_id[:20]}... (normal for new window)")
            else:
                print(f"[best_ask error] token={token_id[:30]}... | {type(e).__name__}: {err_str[:150]}")
            return None

    def buy(self, token_id: str, price: float, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] Would buy → {comment} @ {price:.3f} x {shares}")
            return
        print(f"[BUY] {comment}")
        try:
            order = OrderArgs(token_id=str(token_id), price=price, size=shares, side=BUY)
            signed = self.client.create_order(order)
            resp = self.client.post_order(signed, OrderType.GTC)
            print(f"[BUY SUCCESS] {resp}")
        except Exception as e:
            print(f"[BUY FAILED] {e}")

def get_snapshot(client: PolymarketClient, asset: str, window: str) -> Optional[Snapshot]:
    market = client.find_current_market(asset, window)
    if not market:
        return None
    up = client.best_ask(market["yes_token_id"])
    down = client.best_ask(market["no_token_id"])
    print(f"[{asset.upper()}/{window}] up={up} down={down} | {market.get('question','')[:100]}")
    if up is None or down is None:
        return None
    return Snapshot(asset=asset, yes_token=market["yes_token_id"], no_token=market["no_token_id"], up_ask=up, down_ask=down)

def check_window(client: PolymarketClient, window: str):
    btc = get_snapshot(client, "BTC", window)
    eth = get_snapshot(client, "ETH", window)
    if not (btc and eth):
        return

    threshold = THRESHOLDS.get(window, PRICE_GAP_THRESHOLD)
    gap = btc.up_ask - eth.up_ask
    print(f"[gap/{window}] BTC_up={btc.up_ask:.2f} ETH_up={eth.up_ask:.2f} diff={gap:+.2f} (thresh ±{threshold})")

    if gap <= -threshold:
        print(f"[SIGNAL/{window}] BTC UP cheap → BUY BTC UP + ETH DOWN")
        client.buy(btc.yes_token, btc.up_ask, SHARES_PER_TRADE, f"BTC UP @ {btc.up_ask:.3f} [{window}]")
        client.buy(eth.no_token, eth.down_ask, SHARES_PER_TRADE, f"ETH DOWN @ {eth.down_ask:.3f} [{window}]")
    elif gap >= threshold:
        print(f"[SIGNAL/{window}] ETH UP cheap → BUY ETH UP + BTC DOWN")
        client.buy(eth.yes_token, eth.up_ask, SHARES_PER_TRADE, f"ETH UP @ {eth.up_ask:.3f} [{window}]")
        client.buy(btc.no_token, btc.down_ask, SHARES_PER_TRADE, f"BTC DOWN @ {btc.down_ask:.3f} [{window}]")

def run():
    print(f"[bot] BTC/ETH Correlation Arbitrage started | Dry-run: {DRY_RUN}")
    client = PolymarketClient()
    while True:
        try:
            for window in WINDOWS:
                check_window(client, window)
            time.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            time.sleep(30)

if __name__ == "__main__":
    run()
