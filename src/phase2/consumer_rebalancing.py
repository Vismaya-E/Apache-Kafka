import json
import time
from kafka import KafkaConsumer

instance_name = input("Enter Consumer Name: ")


def _safe_deserializer(x):
    if not x:
        return None
    try:
        return json.loads(x.decode("utf-8"))
    except Exception:
        try:
            return x.decode("utf-8")
        except Exception:
            return None


consumer = KafkaConsumer(
    "ecommerce.orders",
    bootstrap_servers="localhost:9092",
    group_id="order-analytics-rebalance",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=_safe_deserializer,
)

print(f"\n{instance_name} started...\n")

last_assignment = None

while True:
    consumer.poll(timeout_ms=1000)

    current_assignment = sorted(
        [p.partition for p in consumer.assignment()]
    )

    if current_assignment != last_assignment:
        print(f"\n[{instance_name}] Assigned Partitions: {current_assignment}")
        last_assignment = current_assignment

    time.sleep(1)