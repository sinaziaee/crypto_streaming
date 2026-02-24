from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from config import BOOTSTRAP_SERVERS, TOPIC_CONFIGS



def ensure_topics():
    admin_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    admin = AdminClient(admin_config)

    new_topics = []

    for _, config in TOPIC_CONFIGS.items():
        extra_config = {
            "retention.ms": config["retention_ms"],
            "cleanup.policy": config["cleanup_policy"],
            "retention.bytes": config["retention_bytes"],
        }
        new_topics.append(NewTopic(topic=config["topic"], num_partitions=config["num_partitions"], replication_factor=config["replication_factor"], config=extra_config))
        print(f"Creating topic {config['topic']} with {config['num_partitions']} partitions and {config['replication_factor']} replication factor")

if __name__ == "__main__":
    ensure_topics()