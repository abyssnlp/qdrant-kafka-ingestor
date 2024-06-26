import json
import random
import time
from kafka import KafkaProducer


def generate_random_data():
    """Generate random dictionary data"""
    return {f"key_{random.randint(1, 100)}": random.randint(1, 1000) for _ in range(5)}


def produce_messages(naive=True):
    """Produce messages
    - naive: simple k-v pairs
    - structured: QdrantPoint
    """

    count = 10

    if naive:
        while True:
            # Generate message key and value
            key = count
            value = generate_random_data()

            # Send message to Kafka
            producer.send("tester", key=key, value=value)
            time.sleep(1)
            count += 1
    else:
        while True:
            key = count
            value = {
                "id": count,
                "vector": [random.randint(1, 1000) for _ in range(5)],
                "payload": generate_random_data(),
            }

            producer.send("tester", key=key, value=value)
            time.sleep(0.2)
            count += 1


if __name__ == "__main__":
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        key_serializer=lambda k: k.to_bytes(4, byteorder="big"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    produce_messages(naive=False)
