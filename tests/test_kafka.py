from kafka import KafkaProducer


def test_kafka_connection():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092"
    )

    assert producer.bootstrap_connected()

    producer.close()