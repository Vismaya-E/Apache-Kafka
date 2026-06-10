import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ride.events"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

drivers = ["D001", "D002", "D003", "D004", "D005"]
riders = ["U001", "U002", "U003", "U004", "U005"]

statuses = [
    "REQUESTED",
    "ACCEPTED",
    "COMPLETED",
    "CANCELLED"
]

ride_number = 1

print("Ride Producer Started...\n")

for ride_number in range(1, 21):

    ride = {
        "ride_id": f"R{ride_number:03}",
        "driver_id": random.choice(drivers),
        "rider_id": random.choice(riders),
        "status": random.choice(statuses),
        "lat": round(random.uniform(12.90, 13.10), 6),
        "lon": round(random.uniform(77.50, 77.70), 6),
        "timestamp": datetime.now().isoformat()
    }

    producer.send(TOPIC, ride)

    print("Produced:", ride)

    ride_number += 1

    time.sleep(2)