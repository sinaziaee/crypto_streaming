BOOTSTRAP_SERVERS = "localhost:9092"

PRICE_TRACKER_TOPIC = "crypto.ticker.raw"
WS_CONTROL_TOPIC = "crypto.ws.control"  # heartbeats, status, other non-ticker messages

TOPIC_CONFIGS = {
    "price_tracker_topic": {
        "topic": PRICE_TRACKER_TOPIC,
        "num_partitions": 3,
        "replication_factor": 1,
        "retention_ms": 604800000,  # 7 days
        "cleanup_policy": "delete",
        "retention_bytes": 1073741824,  # 1GB
    },
    "ws_control_topic": {
        "topic": WS_CONTROL_TOPIC,
        "num_partitions": 1,
        "replication_factor": 1,
        "retention_ms": 3600000,  # 1 hour
        "cleanup_policy": "delete",
        "retention_bytes": 10485760,  # 10MB
    },
}

PRICE_TRACKER_PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
}

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

KRAKEN_WS_SYMBOLS = ["BTC/USD", "ETH/USD", "XRP/USD"]
