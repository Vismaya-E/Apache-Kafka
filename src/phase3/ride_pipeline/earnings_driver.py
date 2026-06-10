import json
from collections import defaultdict
from kafka import KafkaConsumer

EARNING_PER_RIDE = 5

consumer = KafkaConsumer(
    "ride.completed",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000
)

print("Driver Earnings Consumer Started...\n")

completed_rides = defaultdict(int)

for message in consumer:
    ride = message.value
    driver_id = ride["driver_id"]

    completed_rides[driver_id] += 1

consumer.close()

print("\nFINAL DRIVER EARNINGS REPORT")
print("=" * 45)

for driver_id, rides in sorted(completed_rides.items()):
    earnings = rides * EARNING_PER_RIDE
    print(
        f"Driver: {driver_id} | "
        f"Rides: {rides} | "
        f"Earnings: ${earnings}"
    )

print("=" * 45)