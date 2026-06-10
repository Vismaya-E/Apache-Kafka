import json
from kafka import KafkaConsumer
from kafka import TopicPartition

consumer_name = input("Enter Consumer Name: ")

def print_partitions(consumer, name):
    parts = consumer.assignment()
    print(f"\n[{name}] Assigned Partitions: {[p.partition for p in parts]}")

# -------------------------------
# Consumer Setup
# -------------------------------
consumer = KafkaConsumer(
    "ecommerce.orders",
    bootstrap_servers="localhost:9092",
    group_id="order-analytics",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x
)

print(f"\n{consumer_name} started...\n")

# -------------------------------
# Rebalance-safe loop
# -------------------------------
import time

while True:
    msg_pack = consumer.poll(timeout_ms=1000)

    print_partitions(consumer, consumer_name)

    for tp, messages in msg_pack.items():
        for message in messages:
            try:
                order = json.loads(message.value.decode("utf-8"))
                print(f"\n[{consumer_name}] Order:", order)

            except json.JSONDecodeError:
                print(f"\n[{consumer_name}] POISON MESSAGE SKIPPED")

    time.sleep(1)