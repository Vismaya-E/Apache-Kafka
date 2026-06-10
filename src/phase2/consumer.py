import json
from collections import defaultdict
from kafka import KafkaConsumer

# Store running count of orders per user
order_count = defaultdict(int)

consumer = KafkaConsumer(
    "ecommerce.orders",
    bootstrap_servers="localhost:9092",
    group_id="order-analytics",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...")

for message in consumer:

    order = message.value

    # Print complete message
    print("\nReceived Order:")
    print(order)

    # Count orders per user
    user_id = order["user_id"]
    order_count[user_id] += 1

    print(f"Orders count for {user_id}: {order_count[user_id]}")
    print("-" * 50)