import argparse
import json
import logging
from pathlib import Path
from .generator import generate_dataset
from .pipeline import run_pipeline
from .spark import create_spark


def main():
    parser = argparse.ArgumentParser(description="SkewShield local PySpark demo")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="Generate reproducible JSONL input")
    gen.add_argument("--output", default="data/generated")
    gen.add_argument("--orders", type=int, default=100_000)
    gen.add_argument("--customers", type=int, default=1_000)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--skew-fraction", type=float, default=0.65)

    run = sub.add_parser("run", help="Run validation, publishing and benchmark")
    run.add_argument("--input", default="data/generated")
    run.add_argument("--output", default="outputs")
    run.add_argument("--run-id", default=None)
    run.add_argument("--hot-customer-id", default="C000001")
    run.add_argument("--salt-buckets", type=int, default=16)
    run.add_argument("--repetitions", type=int, default=3)
    run.add_argument("--master", default="local[*]")
    run.add_argument("--shuffle-partitions", type=int, default=8)
    run.add_argument("--aqe", action=argparse.BooleanOptionalAction, default=True)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.command == "generate":
        result = generate_dataset(args.output, args.orders, args.customers,
                                  args.seed, args.skew_fraction)
        print(json.dumps(result, indent=2))
        return

    spark = create_spark(master=args.master,
                         shuffle_partitions=args.shuffle_partitions,
                         adaptive=args.aqe)
    try:
        result = run_pipeline(
            spark, args.input, args.output, args.run_id,
            args.hot_customer_id, args.salt_buckets, args.repetitions
        )
        print(json.dumps({
            "run_id": result["run_id"],
            "output_dir": result["output_dir"],
            "metrics": result["metrics"],
            "quality": result["quality"],
            "benchmark": {
                k: result["benchmark"][k] for k in [
                    "baseline_median_seconds", "salted_median_seconds",
                    "speedup", "correctness_equal"
                ]
            },
        }, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
