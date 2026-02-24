"""Kraken WebSocket v2 (public) - print BTC/USD last price.
Install: pip install websockets
"""

import asyncio
import json
import websockets


async def connect_kraken_ws():
    uri = "wss://ws.kraken.com/v2"

    async with websockets.connect(uri) as ws:
        print(f"Connected to {uri}")

        # Subscribe to ticker (Level 1) for BTC/USD
        # v2 docs use symbols like "BTC/USD" :contentReference[oaicite:2]{index=2}
        req = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": ["BTC/USD"],
                # optional:
                # "snapshot": True,
                # "event_trigger": "trades",
            },
            "req_id": 1,
        }
        await ws.send(json.dumps(req))
        print("> sent subscribe:", req)

        async for raw in ws:
            # Always print what you get first (helps catch subscribe ack / errors)
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                print("< non-json:", raw)
                continue

            # Print acks / errors / heartbeats etc.
            if "method" in msg and msg.get("method") in ("subscribe", "unsubscribe"):
                print("< ack:", msg)

            if msg.get("success") is False:
                print("< ERROR:", msg)
                continue

            # Ticker payloads look like: {"channel":"ticker","type":"snapshot|update","data":[{...}]}
            # and include "last" price. :contentReference[oaicite:3]{index=3}
            if msg.get("channel") == "ticker" and "data" in msg:
                for item in msg["data"]:
                    symbol = item.get("symbol")
                    last = item.get("last")
                    ts = item.get("timestamp")
                    if last is not None:
                        print(f"BTC price ({symbol}) = {last}  @ {ts}")
            else:
                # Uncomment if you want to see everything:
                # print("<", msg)
                pass


if __name__ == "__main__":
    asyncio.run(connect_kraken_ws())