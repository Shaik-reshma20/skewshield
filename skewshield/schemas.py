from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

ORDER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),
])

TARGET_SCHEMA_DESCRIPTION = {
    "fact_orders": {
        "grain": "One accepted, unique order event",
        "primary_key": ["event_id"],
        "columns": {
            "event_id": "string, non-null, unique event identifier",
            "customer_id": "string, non-null, references dim_customers.customer_id",
            "event_ts": "timestamp, non-null event time in UTC",
            "amount": "double, non-null, non-negative order amount",
            "status": "string, non-null order status",
            "source_file": "string, source file path",
            "run_id": "string, pipeline execution identifier",
        },
    },
    "dim_customers": {
        "grain": "One row per customer",
        "primary_key": ["customer_id"],
        "columns": {
            "customer_id": "string, non-null, unique customer identifier",
            "customer_name": "string",
            "segment": "string",
        },
    },
    "quarantine_orders": {
        "grain": "One rejected source record",
        "columns": {
            "raw_record": "string, original JSON text",
            "source_file": "string, source file path",
            "reject_reason": "string, one or more pipe-separated validation reasons",
            "run_id": "string, pipeline execution identifier",
        },
    },
}
