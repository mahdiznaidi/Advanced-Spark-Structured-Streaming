#!/bin/bash

kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic iot-events \
  --partitions 1 \
  --replication-factor 1
