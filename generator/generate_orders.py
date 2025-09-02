import os
import json
import random
import time
import uuid
import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Read Kafka address from environment, default to localhost:9092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")

PRODUCTS = [
    {"id": "espresso", "price": 2.50},
    {"id": "americano", "price": 3.00},
    {"id": "latte", "price": 3.80},
    {"id": "cappuccino", "price": 3.80},
    {"id": "mocha", "price": 4.20},
]
STORES = ["store_istanbul_01", "store_ankara_02", "store_izmir_03"]

def connect_producer(max_wait_sec=30):
    """Try to connect to Kafka, retrying for up to max_wait_sec seconds."""
    start = time.time()
    last_err = None
    while time.time() - start < max_wait_sec:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                linger_ms=50,
            )
        except NoBrokersAvailable as e:
            last_err = e
            print("Kafka not ready yet at", KAFKA_BOOTSTRAP, "- retrying...")
            time.sleep(1.5)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}") from last_err

def make_order():
    prod = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    return {
        "order_id": str(uuid.uuid4()),
        "store_id": random.choice(STORES),
        "product_id": prod["id"],
        "unit_price": prod["price"],
        "quantity": qty,
        "total_price": round(prod["price"] * qty, 2),
        "event_time": datetime.datetime.utcnow().isoformat() + "Z"
    }

if __name__ == "__main__":
    print(f"Connecting producer to {KAFKA_BOOTSTRAP} ...")
    producer = connect_producer()
    print(f"Producing JSON orders to topic '{TOPIC}'")
    while True:
        order = make_order()
        # Use store_id as the message key (good habit for partition locality)
        producer.send(TOPIC, key=order["store_id"], value=order)
        print("→", order)
        time.sleep(0.7)  # slow enough to watch the stream