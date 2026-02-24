import asyncio
import sys
from pathlib import Path

# Allow imports from project root (config) and services/ (kafka_init) when run from any cwd
_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root.parent))  # project root
sys.path.insert(0, str(_root))         # services/

import websockets
from confluent_kafka import Producer, KafkaException
from config import BOOTSTRAP_SERVERS, PRICE_TRACKER_PRODUCER_CONFIG, TOPIC_CONFIGS
from kafka_init import ensure_topics
import json

from config import KRAKEN_WS_URL, KRAKEN_WS_SYMBOLS


def _delivery_callback(err, msg):
    """Called once per message when produce() result is known (from poll/flush)."""
    if err is not None:
        topic = msg.topic() if msg else "?"
        key = msg.key() if msg and msg.key() else None
        print(f"Produce failed: topic={topic} key={key!r} error={err}")
    else:
        print(f"Produced: {msg.value()}")

class PriceTrackerProducer:
    def __init__(self):
        self.producer = Producer(PRICE_TRACKER_PRODUCER_CONFIG)
        self.topic = TOPIC_CONFIGS["price_tracker_topic"]["topic"]
        self.control_topic = TOPIC_CONFIGS["ws_control_topic"]["topic"]
        self.ws = None
        self.subscriptions = set()
        self.channel = "ticker"
        self.req_id = 1
        # ensure the topic is created
        ensure_topics()

    async def subscribe_to_symbol(self, symbol: str):
        try:
            req = {
                "method": "subscribe",
                "params": {
                    "channel": self.channel,
                    "symbol": [symbol],
                },
                "req_id": self.req_id,
            }
            await self.ws.send(json.dumps(req))
            print(f"Subscribed to {symbol}")
            self.subscriptions.add(symbol)
            self.req_id += 1
        except Exception as e:
            print(f"Error subscribing to {symbol}: {e}")
            raise

    async def unsubscribe_from_symbol(self, symbol: str):
        try:
            req = {
                "method": "unsubscribe",
                "params": {
                    "channel": self.channel,
                    "symbol": [symbol],
                },
                "req_id": self.req_id,
            }
            await self.ws.send(json.dumps(req))
            print(f"Unsubscribed from {symbol}")
            self.subscriptions.remove(symbol)
            self.req_id += 1
        except Exception as e:
            print(f"Error unsubscribing from {symbol}: {e}")
            raise

    def _produce(self, value: bytes, key: str | None = None, *, topic: str | None = None):
        """Sync produce; run via asyncio.to_thread to avoid blocking the event loop."""
        target_topic = topic or self.topic
        key_bytes = key.encode("utf-8") if key else None
        try:
            self.producer.produce(
                topic=target_topic,
                value=value,
                key=key_bytes,
                on_delivery=_delivery_callback,
            )
        except KafkaException as e:
            print(f"Produce failed (sync): topic={target_topic} key={key!r} error={e}")
            return
        self.producer.poll(0)  # invoke delivery callbacks

    async def _send_to_kafka(self, message: str):
        """Parse message and produce to the appropriate topic (ticker vs control)."""
        try:
            data = json.loads(message)
            # Skip subscription request/response only (we don't store those)
            if data.get("method") or "result" in data:
                return
            channel = data.get("channel")
            # Control channels → dedicated topic (heartbeat, status, etc.)
            if channel in ("heartbeat", "status"):
                await asyncio.to_thread(
                    self._produce, message.encode("utf-8"), key=channel, topic=self.control_topic
                )
                return
            # Ticker (and any other business channels) → main topic
            # Use symbol as key for partitioning (same symbol -> same partition)
            key = None
            payload = data.get("data")
            if isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], dict):
                key = payload[0].get("symbol") or payload[0].get("pair")
            elif "symbol" in data:
                key = data["symbol"]
            elif "pair" in data:
                key = data["pair"]
            await asyncio.to_thread(self._produce, message.encode("utf-8"), key)
        except json.JSONDecodeError:
            await asyncio.to_thread(self._produce, message.encode("utf-8"), None)
        except Exception as e:
            print(f"Error producing to Kafka: {e}")

    async def run(self):
        async with websockets.connect(KRAKEN_WS_URL) as self.ws:
            print(f"Connected to {KRAKEN_WS_URL}")
            for symbol in KRAKEN_WS_SYMBOLS:
                await self.subscribe_to_symbol(symbol)
            try:
                while True:
                    message = await self.ws.recv()
                    await self._send_to_kafka(message)
                    
            except websockets.ConnectionClosed:
                print("WebSocket connection closed")
            except KeyboardInterrupt as e:
                print("Keyboard interrupt received")
            except Exception as e:
                print(f"Error producing to Kafka: {e}")
            finally:
                for symbol in list(self.subscriptions):
                    try:
                        await self.unsubscribe_from_symbol(symbol)
                    except Exception:
                        pass
                await asyncio.to_thread(self.producer.flush)
            print("Price tracker stopped")


async def main():
    producer = PriceTrackerProducer()
    await producer.run()


if __name__ == "__main__":
    asyncio.run(main())
