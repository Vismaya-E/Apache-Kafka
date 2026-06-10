import json
import pandas as pd
from kafka import KafkaProducer


df = pd.read_csv("data/ecommerce_orders.csv")
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for _, row in df.iterrows():
    producer.send("ecommerce.orders",
              key=str(row["order_id"]).encode("utf-8"),
                value=json.dumps(row.to_dict()).encode("utf-8"))
    
    print(f"Sent Order: {row['order_id']}")


producer.flush()
print("All Messages sent successfully")
