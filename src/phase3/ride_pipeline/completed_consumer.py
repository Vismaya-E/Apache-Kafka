import json
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    "ride.events",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="ride-completed-demo",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Completed Ride Consumer Started...\n")

completed_count = 0

for message in consumer:

    ride = message.value

    if ride["status"] == "COMPLETED":

        completed_count += 1

        producer.send("ride.completed", ride)
        producer.flush()

        print(f"\nCOMPLETED RIDE #{completed_count}")
        print(ride)
        print("✓ Sent to ride.completed")
        print("-" * 50)