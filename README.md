# SkewShield — Reproducible Spark Data Skew Benchmark

SkewShield is a local-first PySpark project that creates a reproducible skewed
order workload, validates and quarantines bad records, compares a standard
Spark join with a salted join, and writes correctness and operational reports.

It is intentionally scoped as a tested vertical slice rather than a production
platform. All data is synthetic and generated locally.

## 1. Problem and solution

A small number of customer keys receive a disproportionate share of order
events. A normal join/groupBy can create uneven shuffle partitions: a few
tasks process much more data than others. SkewShield compares:

- **Baseline:** standard equi-join followed by aggregation.
- **Mitigation:** salt the hot fact-side key into N buckets and replicate only
  that hot dimension row across those buckets.
- **Correctness:** compare per-segment order counts and revenue exactly after
  rounding to cents.

Spark Adaptive Query Execution (AQE) is enabled by default. For a useful
experiment, run with AQE both enabled and disabled and record the setting.
Salting is not guaranteed to win on every laptop or dataset; overhead can make
it slower for small workloads.

## 2. Architecture

```text
Fixed-seed JSONL generator
        |
        v
Raw orders + customer dimension
        |
        v
Spark JSON parsing and schema checks
        |
        +----> Quarantine (malformed, null, invalid, orphan, duplicate)
        |
        v
Accepted unique fact_orders
        |
        +----> Baseline join + aggregate
        |
        +----> Salted join + aggregate
        |
        v
Output Parquet + reconciliation + benchmark + run metrics
```

## 3. Requirements

- Python 3.10, 3.11, or 3.12
- Java 8/11/17 available on `PATH` (Java 17 is recommended for Spark 3.5)
- Git
- Local disk space for generated input and Parquet output

Check the environment:

```bash
python --version
java -version
```

## 4. Setup

### macOS / Linux

```bash
git clone https://github.com/Shaik-reshma20/skewshield.git
cd skewshield
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Windows PowerShell

```powershell
git clone https://github.com/Shaik-reshma20/skewshield.git
cd skewshield
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 5. Generate data

Quick sample (10,000 events):

```bash
python -m skewshield.cli generate --output data/generated --orders 10000 --customers 100
```

Benchmark workload (100,000 events):

```bash
python -m skewshield.cli generate --output data/generated --orders 100000 --customers 1000 --seed 42 --skew-fraction 0.65
```

The generator creates:
- 1 customer dimension JSONL file
- An order JSONL file with a fixed-seed key distribution
- A 65% hot-key concentration by default
- Intentional malformed JSON, null, duplicate, negative amount, and orphan-key
  examples to exercise quarantine logic

The default hot key is `C000001`. If you change the customer count or hot key,
ensure the hot key exists in the generated dimension.

## 6. Run the pipeline

```bash
python -m skewshield.cli run \
  --input data/generated \
  --output outputs \
  --run-id demo-001 \
  --hot-customer-id C000001 \
  --salt-buckets 16 \
  --repetitions 3
```

On Windows PowerShell, use a single line or PowerShell backticks for line
continuation.

To compare AQE settings:

```bash
python -m skewshield.cli run --input data/generated --output outputs --run-id aqe-on --aqe
python -m skewshield.cli run --input data/generated --output outputs --run-id aqe-off --no-aqe
```

To vary local parallelism, use `--master local[2]` or `--master local[*]`.
Keep all other settings fixed when comparing runs.

## 7. Outputs

Each run writes to `outputs/run_id=<run-id>/`:

| Output | Purpose |
|---|---|
| `fact_orders/` | Accepted, unique order events in Parquet |
| `dim_customers/` | Customer dimension in Parquet |
| `quarantine_orders/` | Original raw record, source file, reason, run ID |
| `run_metrics.json` | Status, timestamps, duration, row counts, failure reason |
| `reconciliation.json` | Count, uniqueness, non-negative and FK assertions |
| `performance_comparison.json` | Per-run timings, medians, speedup, result comparison |

The benchmark JSON is generated from real Spark actions. Do not copy sample
numbers into a submission: run the workload on the target machine and report
the resulting measurements.

## 8. Data dictionary and rules

See [`docs/data_dictionary.md`](docs/data_dictionary.md).

Key rules:
- `event_id` is the deterministic event primary key.
- `customer_id` must exist in `dim_customers`.
- `event_ts`, `amount`, and `status` are required.
- `amount` must be non-negative.
- Exact duplicate `event_id` rows are reduced to one; the extra copy is
  quarantined. The chosen row is deterministic by lexicographic raw JSON.
- Unknown customer references are quarantined.
- Late/out-of-order events are retained. This batch example does not impose a
  watermark or silently discard old events.
- Malformed JSON, missing fields, and unexpected source fields are quarantined
  with source traceability. Unknown JSON keys are treated as schema drift rather
  than silently ignored.

## 9. Idempotency, retry and backfill

- A caller-supplied `run_id` identifies one logical run.
- Outputs for the same run ID are overwritten, so retrying that run does not
  append duplicate output files.
- Different run IDs preserve separate run folders for audit/history.
- Input generation is deterministic for the same parameters and seed.
- Backfill by generating or supplying a bounded input folder and using a new
  run ID. Keep the source files immutable for reproducibility.
- The pipeline records `FAILED` status and the exception text if a run fails.
  A retry with the same run ID replaces that run's partial outputs.

This is local filesystem idempotency, not a transactional multi-table commit.
For production, use Delta Lake/Iceberg/Hudi transactions and a durable run
ledger.

## 10. Tests

```bash
pytest
```

Coverage includes:
- Deterministic generation
- Happy-path accepted event
- Empty input
- Malformed JSON
- Missing amount
- Negative amount
- Duplicate event key
- Invalid foreign key
- Salted and baseline aggregate equivalence
- Salt parameter validation
- Data dictionary contract

## 11. Performance methodology and interpretation

The benchmark caches the same accepted fact and customer dimension before
measuring both strategies, performs Spark actions with `collect()`, and records
multiple wall-clock timings. It reports medians and the ratio:

`speedup = baseline_median_seconds / salted_median_seconds`

Interpretation:
- A value above 1 means the salted implementation was faster in that run.
- A value below 1 means salting was slower.
- Small local workloads are sensitive to JVM startup, cache state, CPU load,
  shuffle partition count, and filesystem effects.
- For stronger evidence, increase to 500K–1M events, use at least 3–5
  repetitions, inspect the Spark UI task-duration and shuffle metrics, and
  compare AQE on/off.
- This benchmark is a join-skew demonstration; it does not claim that salting
  always beats AQE or is universally optimal.

### Actual benchmark run — 2026-11-10

Measured using run ID `linux-demo-001`.

| Setting | Value |
|---|---:|
| Input rows | 100,006 |
| Accepted orders | 100,000 |
| Rejected records | 6 |
| Customers | 1,000 |
| Hot-key share | 65% |
| Salt buckets | 16 |
| Shuffle partitions | 8 |
| AQE | Enabled |
| Environment | Ubuntu 26.04.1 LTS (WSL2) |
| Python / Java / PySpark | 3.11.18 / 17.0.20.1 / 3.5.5 |

| Benchmark metric | Baseline | Salted |
|---|---:|---:|
| Median runtime | 0.6547 s | 0.9071 s |
| Aggregated order count | 100,000 | 100,000 |
| Aggregated revenue | 25,097,777.04 | 25,097,777.04 |

**Measured speedup: 0.7218x.** Salting was approximately 38.5% slower
in this local run. Both strategies produced identical aggregate results.
Data reconciliation passed: 100,006 input records = 100,000 accepted +
6 rejected.

This result illustrates that salting is workload-dependent. Its additional
redistribution work can outweigh skew mitigation benefits on smaller local
workloads. No Spark UI task-level skew measurements were captured, so no
partition-level performance improvement is claimed.

Detailed measurements: [`docs/benchmark_results.md`](docs/benchmark_results.md).

## 12. Known limitations / next steps

- JSONL is used for transparent local input and malformed-line testing.
- The local filesystem is used instead of a transactional lakehouse.
- The benchmark is a microbenchmark; machine-specific timing is not portable.
- Add structured streaming and watermarks if the source becomes continuous.
- Add Delta Lake/Iceberg, schema registry, CI, and a persistent run ledger for
  production deployment.
- Consider Spark event logs and Spark UI evidence for deeper partition-level
  diagnosis.

## 13. Submission checklist

- [x] Run the generator and pipeline from a clean Linux virtual environment.
- [x] Run `pytest` — all 7 tests passed.
- [x] Inspect `reconciliation.json`; all assertions are true.
- [x] Inspect `performance_comparison.json`; `correctness_equal` is true.
- [x] Add actual benchmark results and machine details to
      `docs/benchmark_results.md`.
- [ ] Push the source to a **public** GitHub repository.
- [ ] Paste the public repository URL into the YottaFlex application.

## License

MIT. Synthetic data only.
