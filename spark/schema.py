"""
This file defines the expected schema of the streaming JSON events.

You will use this schema in streaming_app.py to:
- Parse raw JSON messages
- Enable event-time processing
- Detect malformed or incomplete records
"""

from pyspark.sql.types import *

event_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("event_time", TimestampType(), True)
])
