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
SHARES_PER_TRADE = get_env_float("SHARES_PER_TRADE", 10.0)
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", 30))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

class PolymarketClient:
    def __init__(self):
        self.host = "https://clob.polymarket.com"
        self.chain_id = 137
        private_key = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER")
        if not private_key or not funder:
            raise ValueError("PRIVATE_KEY and FUNDER environment variables are required")

        self.client = ClobClient(
            host=self.host,
            key=private_key,
            chain_id=self.chain_id,
            funder=funder,
            signature_type=0  # 0 = MetaMask / EOA
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())
        print("[bot] PolymarketClient initialized successfully")

    def find_market(self, asset: str, window: str) -> Optional[dict]:
        minutes_map = {"5m": 5, "15m": 15, "4h": 240}
        minutes = minutes_map.get(window)
        if not minutes:
            return None
        interval_sec = minutes * 60
        ts = int(time.time() // interval_sec * interval_sec)
        slug = f"{asset.lower()}-updown-{window}-{ts}"
        url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if not data:
                return None
            market = data[0] if isinstance(data, list) else data

            # Extract token IDs (works for both clobTokenIds and tokens format)
            if "clobTokenIds" in market and len(market["clobTokenIds"]) == 2:
                yes_token = str(market["clobTokenIds"][0])
                no_token = str(market["clobTokenIds"][1])
            else:
                tokens = market.get("tokens", [])
                yes_token = next((str(t.get("token_id")) for t in tokens if t.get("outcome") in ["Yes", "1"]), None)
                no_token = next((str(t.get("token_id")) for t in tokens if t.get("outcome") in ["No", "0"]), None)

            if not (yes_token and no_token):
                return None
            return {
                "yes_token_id": yes_token,
                "no_token_id": no_token,
                "question": market.get("question", f"{asset} {window}")
            }
        except Exception as e:
            print(f"[find_market error] {e}")
            return None

    def best_ask(self, token_id: str) -> Optional[float]:
        try:
            # best price to BUY the token = best ASK
            price = self.client.get_price(token_id, side="BUY")
            return float(price) if price is not None else None
        except:
            return None

    def buy(self, token_id: str, price: float, shares: float, comment: str):
        if DRY_RUN:
            print(f"[DRY RUN] Would buy {comment} @ {price:.3f} x {shares} shares")
            return
        print(f"[BUY] {comment} | token={token_id[:12]}... price={price:.3f} shares={shares}")
        try:
            order = OrderArgs(token_id=token_id, price=price, size=shares, side=BUY)
            signed = self.client.create_order(order)
            resp = self.client.post_order(signed, OrderType.GTC)
            print(f"[BUY SUCCESS] {resp}")
        except Exception as e:
            print(f"[BUY ERROR] {e}")

def get_snapshot(client: PolymarketClient, asset: str, window: str) -> Optional[Snapshot]:
    market = client.find_market(asset, window)
    if not market:
        return None
    up = client.best_ask(market["yes_token_id"])
    down = client.best_ask(market["no_token_id"])
    print(f"[{asset}/{window}] q='{market['question'][:60]}'  up={up}  down={down}")
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
    print(f"[gap/{window}] BTC_up={btc.up_ask:.2f} ETH_up={eth.up_ask:.2f} diff={gap:+.2f} threshold=±{threshold}")

    if gap <= -threshold:
        print(f"[signal/{window}] BTC up cheap → BUY BTC UP + ETH DOWN")
        client.buy(btc.yes_token, btc.up_ask, SHARES_PER_TRADE, f"BTC UP @ {btc.up_ask:.3f} [{window}]")
        client.buy(eth.no_token, eth.down_ask, SHARES_PER_TRADE, f"ETH DOWN @ {eth.down_ask:.3f} [{window}]")
    elif gap >= threshold:
        print(f"[signal/{window}] ETH up cheap → BUY ETH UP + BTC DOWN")
        client.buy(eth.yes_token, eth.up_ask, SHARES_PER_TRADE, f"ETH UP @ {eth.up_ask:.3f} [{window}]")
        client.buy(btc.no_token, btc.down_ask, SHARES_PER_TRADE, f"BTC DOWN @ {btc.down_ask:.3f} [{window}]")

def run():
    print(f"[bot] Starting BTC/ETH correlation arbitrage bot")
    print(f"   Windows: {WINDOWS} | Thresholds: {THRESHOLDS} | Shares: {SHARES_PER_TRADE} | Poll: {POLL_INTERVAL_SEC}s | Dry-run: {DRY_RUN}\n")
    client = PolymarketClient()
    while True:
        try:
            for window in WINDOWS:
                check_window(client, window)
            time.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")
            traceback.print_exc()
            time.sleep(10)

if __name__ == "__main__":
    run()
