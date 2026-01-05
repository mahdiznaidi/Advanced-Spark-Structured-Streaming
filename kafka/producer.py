# Write your kafka producer code here
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

with open("/data/events_dirty.json") as f:
    for line in f:
        producer.send("iot-events", value=line.strip())
        time.sleep(1)

producer.flush()
