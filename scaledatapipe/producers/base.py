"""Kafka producer helpers shared by all producers."""
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

from scaledatapipe.common import config


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def send_all(topic: str, events: list[dict]) -> None:
    producer = make_producer()
    for event in events:
        producer.send(topic, event)
    producer.flush()
    producer.close()
    print(f"[{topic}] sent {len(events)} events")
