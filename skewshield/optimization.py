from pyspark.sql import functions as F


def baseline_aggregate(orders, customers):
    joined = orders.join(customers, "customer_id", "inner")
    return (joined.groupBy("segment")
            .agg(F.count("*").alias("order_count"),
                 F.round(F.sum("amount"), 2).alias("revenue"))
            .orderBy("segment"))


def salted_aggregate(orders, customers, hot_customer_id, salt_buckets=16):
    """Salt only the hot key; replicate its dimension row across salt buckets."""
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be >= 2")

    salted_orders = orders.withColumn(
        "_salt",
        F.when(
            F.col("customer_id") == F.lit(hot_customer_id),
            F.pmod(F.xxhash64("event_id"), F.lit(salt_buckets))
        ).otherwise(F.lit(0))
    )

    hot_dimension = (customers
        .filter(F.col("customer_id") == hot_customer_id)
        .crossJoin(customers.sparkSession.range(salt_buckets))
        .withColumnRenamed("id", "_salt"))

    normal_dimension = (customers
        .filter(F.col("customer_id") != hot_customer_id)
        .withColumn("_salt", F.lit(0)))

    salted_dimension = normal_dimension.unionByName(hot_dimension)
    joined = salted_orders.join(salted_dimension, ["customer_id", "_salt"], "inner")
    return (joined.groupBy("segment")
            .agg(F.count("*").alias("order_count"),
                 F.round(F.sum("amount"), 2).alias("revenue"))
            .orderBy("segment"))
