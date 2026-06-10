import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,

    # Reliability settings
    acks="all",
    retries=5,

    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Reliable Ride Producer Started...\n")

STATUSES = [
    "REQUESTED",
    "ACCEPTED",
    "COMPLETED",
    "CANCELLED"
]

for i in range(1, 21):

    ride = {
        "ride_id": f"R{i:03}",
        "driver_id": f"D{random.randint(1,5):03}",
        "rider_id": f"U{random.randint(1,5):03}",
        "status": random.choice(STATUSES),
        "lat": round(random.uniform(12.90, 13.10), 6),
        "lon": round(random.uniform(77.50, 77.70), 6),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("ride.events", ride)

    print(f"Produced: {ride}")

    time.sleep(2)

producer.flush()
producer.close()

print("\nAll ride events sent successfully.")