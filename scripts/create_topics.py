"""Create the pipeline's Kafka topics (idempotent)."""
from kafka.admin import KafkaAdminClient, NewTopic

from scaledatapipe.common import config


def main():
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP)
    existing = set(admin.list_topics())
    missing = [t for t in config.TOPICS if t not in existing]
    if missing:
        admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in missing])
    print(f"created: {missing or 'none'} | all topics: {sorted(set(admin.list_topics()) & set(config.TOPICS))}")
    admin.close()


if __name__ == "__main__":
    main()
