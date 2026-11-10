import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F
from .benchmark import run_benchmark
from .generator import generate_dataset
from .validation import load_and_validate_orders, load_customers

LOG = logging.getLogger("skewshield")


def _write_json(path, obj):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def run_pipeline(spark, input_dir, output_dir, run_id=None, hot_customer_id="C000001",
                 salt_buckets=16, repetitions=3):
    run_id = run_id or str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    out = Path(output_dir) / f"run_id={run_id}"
    out.mkdir(parents=True, exist_ok=True)
    metrics = {
        "run_id": run_id,
        "status": "RUNNING",
        "started_at": started.isoformat(),
        "input_dir": str(Path(input_dir).resolve()),
    }
    _write_json(out / "run_metrics.json", metrics)

    try:
        customers = load_customers(spark, str(Path(input_dir) / "customers.jsonl"))
        accepted, rejects = load_and_validate_orders(
            spark, str(Path(input_dir) / "orders.jsonl"), customers, run_id
        )

        input_count = spark.read.text(str(Path(input_dir) / "orders.jsonl")).count()
        accepted_count = accepted.count()
        reject_count = rejects.count()

        accepted.write.mode("overwrite").parquet(str(out / "fact_orders"))
        customers.dropDuplicates(["customer_id"]).write.mode("overwrite").parquet(
            str(out / "dim_customers")
        )
        rejects.write.mode("overwrite").parquet(str(out / "quarantine_orders"))

        # Reconciliation: every parsed source line must be accepted or quarantined.
        reconciled = accepted_count + reject_count == input_count
        if not reconciled:
            raise AssertionError(
                f"Input reconciliation failed: {input_count} != "
                f"{accepted_count} + {reject_count}"
            )

        benchmark = run_benchmark(
            accepted, customers, hot_customer_id, salt_buckets, repetitions,
            str(out / "performance_comparison.json")
        )
        quality = {
            "input_rows": input_count,
            "accepted_rows": accepted_count,
            "rejected_rows": reject_count,
            "reconciled": reconciled,
            "accepted_unique_event_ids": (
                accepted.select("event_id").distinct().count() == accepted_count
            ),
            "accepted_nonnegative_amounts": (
                accepted.filter(F.col("amount") < 0).count() == 0
            ),
            "accepted_customer_references_valid": (
                accepted.select("customer_id").distinct()
                .join(customers.select("customer_id").distinct(),
                      "customer_id", "left_anti").count() == 0
            ),
            "baseline_equals_salted": benchmark["correctness_equal"],
        }
        _write_json(out / "reconciliation.json", quality)

        metrics.update({
            "status": "SUCCESS",
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "input_rows": input_count,
            "accepted_rows": accepted_count,
            "rejected_rows": reject_count,
            "failure_reason": None,
        })
        _write_json(out / "run_metrics.json", metrics)
        LOG.info("Run %s completed: input=%s accepted=%s rejected=%s",
                 run_id, input_count, accepted_count, reject_count)
        return {"run_id": run_id, "output_dir": str(out), "metrics": metrics,
                "quality": quality, "benchmark": benchmark}
    except Exception as exc:
        metrics.update({
            "status": "FAILED",
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "failure_reason": repr(exc),
        })
        _write_json(out / "run_metrics.json", metrics)
        LOG.exception("Run %s failed", run_id)
        raise
