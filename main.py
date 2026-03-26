import os
import time
import traceback
import requests
from dataclasses import dataclass
from typing import Optional

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
SHARES_PER_TRADE = get_env_float("SHARES_PER_TRADE", 5.0)   # Start small
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", 45))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", 2))

class PolymarketClient:
    def __init__(self):
        self.host = "https://clob.polymarket.com"
        private_key = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER")
        if not private_key or not funder:
            raise ValueError("PRIVATE_KEY and FUNDER are required")

        self.client = ClobClient(
            host=self.host,
            key=private_key,
            chain_id=137,
            funder=funder,
            signature_type=SIGNATURE_TYPE
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())
        print(f"[bot] Initialized | Signature Type: {SIGNATURE_TYPE} | Funder: {funder[:10]}... | Dry-run: {DRY_RUN}")

    def find_current_market(self, asset: str, window: str) -> Optional[dict]:
        """Improved: Try recent timestamps to find active market"""
        minutes_map = {"5m": 5, "15m": 15, "4h": 240}
        interval_sec = minutes_map.get(window) * 60
        now = int(time.time())

        # Try current + last 2 intervals (in case of slight timing issues)
        for i in range(3):
            ts = ((now - i * interval_sec) // interval_sec) * interval_sec
            slug = f"{asset.lower()}-updown-{window}-{ts}"
            url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data and (isinstance(data, list) and len(data) > 0 or isinstance(data, dict)):
                        market = data[0] if isinstance(data, list) else data
                        # Extract token IDs
                        if "clobTokenIds" in market and len(market.get("clobTokenIds", [])) == 2:
                            yes_token = str(market["clobTokenIds"][0])
                            no_token = str(market["clobTokenIds"][1])
                            return {
                                "yes_token_id": yes_token,
                                "no_token_id": no_token,
                                "question": market.get("question", f"{asset} {window}")
                            }
            except Exception as e:
                pass  # Silent for retries
        print(f"[find_market] No active {asset}/{window} market found (tried recent slugs)")
        return None

    def best_ask(self, token_id: str) -> Optional[float]:
        try:
            price = self.client.get_price(token_id, side="BUY")
            return float(price) if price is not None else None
        except:
            return None

    def buy(self, token_id: str, price: float, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] Would buy → {comment} @ {price:.3f} x {shares}")
            return
        print(f"[BUY] {comment} | price={price:.3f}")
        try:
            order = OrderArgs(token_id=token_id, price=price, size=shares, side=BUY)
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
    print(f"[{asset}/{window}] up={up} down={down}")
    return Snapshot(
        asset=asset,
        yes_token=market["yes_token_id"],
        no_token=market["no_token_id"],
        up_ask=up,
        down_ask=down,
    )

def check_window(client: PolymarketClient, window: str):
    btc = get_snapshot(client, "BTC", window)
    eth = get_snapshot(client, "ETH", window)
    if not (btc and eth and None not in (btc.up_ask, btc.down_ask, eth.up_ask, eth.down_ask)):
        return

    threshold = THRESHOLDS.get(window, PRICE_GAP_THRESHOLD)
    gap = btc.up_ask - eth.up_ask
    print(f"[gap/{window}] BTC_up={btc.up_ask:.2f} ETH_up={eth.up_ask:.2f} diff={gap:+.2f} (thresh ±{threshold})")

    if gap <= -threshold:
        print(f"[SIGNAL/{window}] BTC up is cheap → buying BTC UP + ETH DOWN")
        client.buy(btc.yes_token, btc.up_ask, SHARES_PER_TRADE, f"BTC UP @ {btc.up_ask:.3f}")
        client.buy(eth.no_token, eth.down_ask, SHARES_PER_TRADE, f"ETH DOWN @ {eth.down_ask:.3f}")
    elif gap >= threshold:
        print(f"[SIGNAL/{window}] ETH up is cheap → buying ETH UP + BTC DOWN")
        client.buy(eth.yes_token, eth.up_ask, SHARES_PER_TRADE, f"ETH UP @ {eth.up_ask:.3f}")
        client.buy(btc.no_token, btc.down_ask, SHARES_PER_TRADE, f"BTC DOWN @ {btc.down_ask:.3f}")

def run():
    print(f"[bot] BTC/ETH Correlation Arbitrage started | Windows: {WINDOWS} | Dry-run: {DRY_RUN}")
    client = PolymarketClient()
    while True:
        try:
            for window in WINDOWS:
                check_window(client, window)
            time.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            print(f"[ERROR] {e}")
            traceback.print_exc()
            time.sleep(10)

if __name__ == "__main__":
    run()
