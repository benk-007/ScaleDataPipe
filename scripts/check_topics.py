"""Report message count and a sample for each pipeline topic.

Replaces the notebook's console-consumer checks, which targeted a non-existent
topic (cyber_threats) and reported Kafka errors as data.
"""
import sys

from kafka import KafkaConsumer, TopicPartition

from scaledatapipe.common import config


def main():
    consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP, consumer_timeout_ms=5000)
    existing = consumer.topics()
    ok = True
    print(f"{'TOPIC':<15} | {'MESSAGES':>8} | SAMPLE")
    for topic in config.TOPICS:
        if topic not in existing:
            print(f"{topic:<15} | {'MISSING':>8} |")
            ok = False
            continue
        partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
        begin, end = consumer.beginning_offsets(partitions), consumer.end_offsets(partitions)
        count = sum(end[p] - begin[p] for p in partitions)
        sample = ""
        if count:
            consumer.assign(partitions)
            for p in partitions:  # seek_to_beginning() is broken in kafka-python 2.2.0
                consumer.seek(p, begin[p])
            record = next(iter(consumer), None)
            sample = record.value.decode("utf-8")[:70] if record else ""
        print(f"{topic:<15} | {count:>8} | {sample}")
        ok = ok and count > 0
    consumer.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
