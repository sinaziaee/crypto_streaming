from confluent_kafka import Consumer, KafkaException
from config import LOG_CONSUMER_CONFIG, PRICE_TRACKER_TOPIC
from schemas import PriceTrackerMessage
from pydantic import ValidationError

from loguru import logger


def pydantic_validator(message: str) -> PriceTrackerMessage:
    try:
        print(message)
        message = PriceTrackerMessage.model_validate_json(message)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        message = PriceTrackerMessage(error=str(e))
    return message


class LogConsumer:
    def __init__(self):
        self.consumer = Consumer(LOG_CONSUMER_CONFIG)
        self.consumer.subscribe([PRICE_TRACKER_TOPIC])

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            message = pydantic_validator(msg.value().decode("utf-8"))
            if message.error:
                continue
            logger.info(f"Message received: {message}")


if __name__ == "__main__":
    consumer = LogConsumer()
    consumer.run()
