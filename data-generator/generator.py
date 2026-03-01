#!/usr/bin/env python3
"""Produces Avro-encoded messages to Kafka topic 'lineage-input'."""

import io
import os
import sys
import time
import uuid

import fastavro
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
TOPIC = os.getenv("KAFKA_TOPIC", "lineage-input")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "10"))
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "10"))

AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "InputEvent",
    "namespace": "com.lineage",
    "fields": [
        {"name": "uuid", "type": "string"},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})


def serialize_avro(record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, AVRO_SCHEMA, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def wait_for_kafka(producer, max_retries=30, delay=2):
    """Wait until Kafka is reachable."""
    for i in range(max_retries):
        try:
            metadata = producer.list_topics(timeout=5)
            if metadata.topics is not None:
                print(f"Kafka is ready (attempt {i + 1})")
                return True
        except Exception as e:
            print(f"Waiting for Kafka (attempt {i + 1}/{max_retries}): {e}")
            time.sleep(delay)
    print("Could not connect to Kafka", file=sys.stderr)
    sys.exit(1)


def main():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 100,
        "batch.num.messages": 1000,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
    }
    producer = Producer(config)

    wait_for_kafka(producer)

    print(f"Producing to topic '{TOPIC}' at ~{RATE_PER_SEC} msgs/sec")
    count = 0
    interval = 1.0 / RATE_PER_SEC

    while True:
        record = {
            "uuid": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
        }
        key = record["uuid"].encode("utf-8")
        value = serialize_avro(record)
        partition = count % NUM_PARTITIONS

        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            partition=partition,
            callback=delivery_report,
        )
        count += 1

        if count % 100 == 0:
            producer.flush()
            print(f"Produced {count} messages")

        producer.poll(0)
        time.sleep(interval)


if __name__ == "__main__":
    main()
