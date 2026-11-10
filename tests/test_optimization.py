from pyspark.sql import Row
from skewshield.optimization import baseline_aggregate, salted_aggregate


def test_salted_join_matches_baseline(spark):
    orders = spark.createDataFrame([
        Row(event_id="E1", customer_id="C1", amount=10.0),
        Row(event_id="E2", customer_id="C1", amount=20.0),
        Row(event_id="E3", customer_id="C2", amount=5.0),
    ])
    customers = spark.createDataFrame([
        Row(customer_id="C1", segment="consumer"),
        Row(customer_id="C2", segment="business"),
    ])
    base = [r.asDict() for r in baseline_aggregate(orders, customers).collect()]
    salted = [r.asDict() for r in salted_aggregate(
        orders, customers, "C1", 4
    ).collect()]
    assert base == salted


def test_salt_bucket_validation(spark):
    import pytest
    from pyspark.sql import Row
    from skewshield.optimization import salted_aggregate
    orders = spark.createDataFrame([Row(event_id="E1", customer_id="C1", amount=1.0)])
    customers = spark.createDataFrame([Row(customer_id="C1", segment="consumer")])
    with pytest.raises(ValueError):
        salted_aggregate(orders, customers, "C1", 1)
