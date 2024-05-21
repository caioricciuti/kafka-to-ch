import time
import random
from confluent_kafka import Producer
import json


def generate_random_data():
    data = {
        "device_id": random.randint(1, 1000000),
        "timestamp": int(time.time()),
        "temperature": format(random.uniform(10.0, 30.0), ".2f"),
        "vibration": format(random.uniform(0.1, 3.0), ".2f"),
    }
    return data


producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} and message key {msg.key()}"
        )


if __name__ == "__main__":
    while True:
        data = generate_random_data()
        producer.produce(
            "iot_data",
            key=str(data["device_id"]),
            value=json.dumps(data),
            callback=delivery_report,
        )
        producer.poll(1)
        time.sleep(1)
