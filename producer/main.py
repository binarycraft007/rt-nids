import csv
import json
import os
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

KAFKA_TOPIC = "network-flows"


def delivery_report(err, msg):
    """Callback triggered by Kafka to check if message was delivered"""
    if err is not None:
        print(f"Message delivery failed: {err}")


def stream_network_data(csv_file_path, target_eps=5000):
    """
    Reads the IDS CSV and streams it to Kafka.
    target_eps = Events Per Second (simulated traffic speed)
    """
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    print(f"Starting stream from {csv_file_path}...")

    with open(csv_file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        count = 0
        start_time = time.time()

        for row in reader:
            payload = json.dumps(row)

            producer.produce(
                topic=KAFKA_TOPIC,
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            count += 1
            if count % 10000 == 0:
                print(f"Sent {count} flows...")

            time.sleep(1.0 / target_eps)

    producer.flush()
    print("Streaming complete.")


if __name__ == "__main__":
    load_dotenv()
    dataset_path = os.getenv("CSV_PATH")

    stream_network_data(dataset_path, target_eps=5000)
