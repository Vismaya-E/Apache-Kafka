from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

# Invalid JSON
producer.send(
    "ecommerce.orders",
    value=b"INVALID_JSON_MESSAGE"
)

producer.flush()

print("Poison message sent.")