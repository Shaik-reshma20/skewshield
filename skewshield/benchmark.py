import json
import statistics
import time
from pathlib import Path
from pyspark.sql import functions as F
from .optimization import baseline_aggregate, salted_aggregate


def _materialize(df):
    # Force execution and return a stable, comparable representation.
    rows = df.collect()
    return sorted(
        [(r["segment"], int(r["order_count"]), round(float(r["revenue"]), 2))
         for r in rows],
        key=lambda x: x[0]
    )


def _measure(fn, repetitions):
    timings, result = [], None
    for _ in range(repetitions):
        start = time.perf_counter()
        result = _materialize(fn())
        timings.append(time.perf_counter() - start)
    return timings, result


def run_benchmark(orders, customers, hot_customer_id, salt_buckets,
                  repetitions, output_path):
    if repetitions < 1:
        raise ValueError("repetitions must be >= 1")

    # Cache inputs so both strategies operate on the same materialized source.
    orders = orders.cache()
    customers = customers.cache()
    orders.count()
    customers.count()

    base_times, base_result = _measure(
        lambda: baseline_aggregate(orders, customers), repetitions
    )
    salt_times, salt_result = _measure(
        lambda: salted_aggregate(orders, customers, hot_customer_id, salt_buckets),
        repetitions
    )

    report = {
        "benchmark": "standard_join_vs_salted_join",
        "repetitions": repetitions,
        "salt_buckets": salt_buckets,
        "hot_customer_id": hot_customer_id,
        "baseline_seconds": base_times,
        "salted_seconds": salt_times,
        "baseline_median_seconds": statistics.median(base_times),
        "salted_median_seconds": statistics.median(salt_times),
        "speedup": (
            statistics.median(base_times) / statistics.median(salt_times)
            if statistics.median(salt_times) > 0 else None
        ),
        "correctness_equal": base_result == salt_result,
        "baseline_result": base_result,
        "salted_result": salt_result,
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps(report, indent=2), encoding="utf-8")
    orders.unpersist()
    customers.unpersist()
    if not report["correctness_equal"]:
        raise AssertionError("Baseline and salted outputs do not reconcile")
    return report
