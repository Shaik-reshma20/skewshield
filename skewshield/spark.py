from pyspark.sql import SparkSession


def create_spark(app_name="SkewShield", master="local[*]", shuffle_partitions=8,
                 adaptive=True):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(adaptive).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(adaptive).lower())
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()
