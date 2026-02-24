from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from config import BOOTSTRAP_SERVERS, TOPIC_CONFIGS



def ensure_topics():
    admin_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    admin = AdminClient(admin_config)

    new_topics = [
        NewTopic(topic, num_partitions=config["num_partitions"], replication_factor=config["replication_factor"])
        for topic, config in TOPIC_CONFIGS.items()
    ]


    fs = admin.create_topics(new_topics, request_timeout=15)

    for topic, f in fs.items():
        try:
            f.result()
            print(f'Topic {topic} created')
        except KafkaException as e:
            # if topic already exists, ignore the error
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"✅ Topic created: {topic}")
            else:
                print(f"❌ Failed to create topic {topic}: {e}")
                raise

if __name__ == "__main__":
    ensure_topics()