import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "ride.completed",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000
)

print("Reading completed rides...\n")

completed_rides = defaultdict(int)

for message in consumer:
    ride = message.value
    driver_id = ride["driver_id"]

    completed_rides[driver_id] += 1

consumer.close()

top_5 = sorted(
    completed_rides.items(),
    key=lambda x: x[1],
    reverse=True
)[:5]

print("TOP 5 DRIVERS BY COMPLETED RIDES")
print("=" * 50)

if not top_5:
    print("No completed rides found.")
else:
    for rank, (driver_id, rides) in enumerate(top_5, start=1):
        print(
            f"{rank}. Driver: {driver_id} | "
            f"Completed Rides: {rides}"
        )

print("=" * 50)