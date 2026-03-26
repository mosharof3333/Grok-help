"""
BTC / ETH Polymarket 5-minute correlation arbitrage bot.

How it works
────────────
BTC and ETH are highly correlated. Their 5-minute prediction markets should
price YES (up) similarly. When they diverge by ≥ 20 cents it means one market
is over-pricing "up" and the other is over-pricing "down".

Example
  BTC up = 40c   BTC down = 61c   →  market thinks BTC likely DOWN
  ETH up = 61c   ETH down = 40c   →  market thinks ETH likely UP

Because they're correlated, only one direction can be correct for both.
We buy the CHEAP side of each:
  • BTC UP  @ 40c  (bet BTC goes up)
  • ETH DOWN @ 40c  (bet ETH goes down, i.e. ETH does NOT go up)

Wait — that's contradictory? No. Re-read:
  If price goes UP  → BTC up  wins → net +60c on BTC, -40c on ETH = +20c
  If price goes DOWN → ETH down wins → -40c on BTC, +60c on ETH = +20c

Total spend: 80c. Guaranteed return: $1 (one side always wins). Profit: 20c.

Trigger condition (same math, both directions):
  BTC_up  is CHEAPER than ETH_up  by ≥ 20c   →  buy BTC_up  + ETH_down
  ETH_up  is CHEAPER than BTC_up  by ≥ 20c   →  buy ETH_up  + BTC_down
"""

import time
import traceback
from dataclasses import dataclass
from typing import Optional

from polymarket_client import PolymarketClient
import config


@dataclass
class Snapshot:
    asset:        str
    yes_token:    str
    no_token:     str
    up_ask:       Optional[float]   # YES price  (up)
    down_ask:     Optional[float]   # NO  price  (down)


WINDOWS = ["5m", "15m", "4h"]

# Per-window gap thresholds (cents)
THRESHOLDS = {"5m": 0.10, "15m": 0.15, "4h": 0.20}


def get_snapshot(client: PolymarketClient, asset: str, window: str) -> Optional[Snapshot]:
    market = client.find_market(asset, window)
    if not market:
        return None

    up   = client.best_ask(market["yes_token_id"])
    down = client.best_ask(market["no_token_id"])
    print(f"[{asset}/{window}] q='{market['question'][:55]}'  up={up}  down={down}")

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

    if not (btc and eth and None not in (btc.up_ask, btc.down_ask,
                                          eth.up_ask, eth.down_ask)):
        return

    threshold = THRESHOLDS.get(window, config.PRICE_GAP_THRESHOLD)
    gap = btc.up_ask - eth.up_ask
    print(f"[gap/{window}]  BTC_up={btc.up_ask:.2f}  ETH_up={eth.up_ask:.2f}  "
          f"diff={gap:+.2f}  (threshold=±{threshold})")

    if gap <= -threshold:
        print(f"[signal/{window}] BTC up cheap / ETH down cheap → entering")
        client.buy(btc.yes_token, btc.up_ask,   config.SHARES_PER_TRADE,
                   f"BTC UP  @ {btc.up_ask:.2f} [{window}]")
        client.buy(eth.no_token,  eth.down_ask, config.SHARES_PER_TRADE,
                   f"ETH DOWN @ {eth.down_ask:.2f} [{window}]")

    elif gap >= threshold:
        print(f"[signal/{window}] ETH up cheap / BTC down cheap → entering")
        client.buy(eth.yes_token, eth.up_ask,   config.SHARES_PER_TRADE,
                   f"ETH UP  @ {eth.up_ask:.2f} [{window}]")
        client.buy(btc.no_token,  btc.down_ask, config.SHARES_PER_TRADE,
                   f"BTC DOWN @ {btc.down_ask:.2f} [{window}]")


def run():
    client = PolymarketClient()
    print(f"[bot] started  windows={WINDOWS}  gap={config.PRICE_GAP_THRESHOLD}  "
          f"shares={config.SHARES_PER_TRADE}  poll={config.POLL_INTERVAL_SEC}s\n")

    while True:
        try:
            for window in WINDOWS:
