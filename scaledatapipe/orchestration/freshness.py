"""Has the streaming job ingested everything produced so far?

Used by the Airflow sensor between the producers and Gold. Reads the streaming
checkpoints over WebHDFS (plain HTTP) so the check needs no JVM:
1. Bronze: the Kafka offsets of its last committed batch reach the end of every
   topic partition.
2. Silver: every Silver and quarantine query committed a batch after Bronze's
   last commit (each Bronze commit with new files triggers one in every query).
"""
import json
from urllib.parse import urlparse

import requests
from kafka import KafkaConsumer, TopicPartition

from scaledatapipe.common import config
from scaledatapipe.medallion import silver

Partition = tuple[str, int]


def silver_queries() -> list[str]:
    return [f"{kind}_{d.name}" for d in silver.DOMAINS for kind in ("silver", "quarantine")]


# --------------------------------------------------------------------- pure logic
def parse_offsets_log(text: str) -> dict[Partition, int]:
    """Kafka offsets from a Structured Streaming offsets/<batch> file.

    Format: "v1", a metadata JSON line, then one JSON line per source; the Kafka
    source line maps topic -> {partition: next offset to read}.
    """
    offsets = {}
    for line in text.strip().splitlines()[2:]:
        for topic, partitions in json.loads(line).items():
            for partition, offset in partitions.items():
                offsets[(topic, int(partition))] = offset
    return offsets


def lagging_partitions(end: dict[Partition, int], committed: dict[Partition, int]) -> list[str]:
    return [f"{t}[{p}] {committed.get((t, p), 0)}/{off}"
            for (t, p), off in sorted(end.items()) if committed.get((t, p), 0) < off]


def stale_queries(bronze_commit_ms: int, commits_ms: dict[str, int | None]) -> list[str]:
    return sorted(q for q, ms in commits_ms.items() if ms is None or ms < bronze_commit_ms)


# --------------------------------------------------------------------- I/O
def _webhdfs(path: str, op: str) -> requests.Response:
    return requests.get(f"{config.WEBHDFS_URL}/webhdfs/v1{path}",
                        params={"op": op, "user.name": "airflow"}, timeout=30)


def latest_commit(query: str) -> tuple[int, int] | None:
    """(batch id, commit time in ms) of a query's last committed batch, or None."""
    r = _webhdfs(f"{urlparse(config.checkpoint_path(query)).path}/commits", "LISTSTATUS")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    batches = [(int(f["pathSuffix"]), f["modificationTime"])
               for f in r.json()["FileStatuses"]["FileStatus"] if f["pathSuffix"].isdigit()]
    return max(batches) if batches else None


def committed_offsets(query: str, batch: int) -> dict[Partition, int]:
    r = _webhdfs(f"{urlparse(config.checkpoint_path(query)).path}/offsets/{batch}", "OPEN")
    r.raise_for_status()
    return parse_offsets_log(r.text)


def kafka_end_offsets() -> dict[Partition, int]:
    consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP)
    try:
        partitions = [TopicPartition(t, p) for t in config.TOPICS
                      for p in (consumer.partitions_for_topic(t) or [])]
        return {(tp.topic, tp.partition): off for tp, off in consumer.end_offsets(partitions).items()}
    finally:
        consumer.close()


def caught_up() -> tuple[bool, str]:
    """(True, summary) once Bronze and Silver have ingested all Kafka data."""
    bronze = latest_commit("bronze")
    end = kafka_end_offsets()
    if bronze is None:
        return (not any(end.values()), "Bronze has not committed any batch yet")
    lag = lagging_partitions(end, committed_offsets("bronze", bronze[0]))
    if lag:
        return False, f"Bronze behind Kafka: {', '.join(lag)}"
    commits = {q: (c[1] if (c := latest_commit(q)) else None) for q in silver_queries()}
    stale = stale_queries(bronze[1], commits)
    if stale:
        return False, f"Silver queries not yet past Bronze batch {bronze[0]}: {', '.join(stale)}"
    return True, f"caught up at Bronze batch {bronze[0]}"
