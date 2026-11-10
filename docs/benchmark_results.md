# Benchmark Results

Measured on the local WSL2 Ubuntu environment using run ID `linux-demo-001`.

## Environment

- OS: Ubuntu 26.04.1 LTS (WSL2)
- CPU: 12 logical processors
- RAM: 3.7 GiB available to WSL
- Python: 3.11.18
- Java: OpenJDK 17.0.20.1
- PySpark: 3.5.5
- Spark master: `local[*]`
- Shuffle partitions: 8
- AQE: Enabled

## Dataset

- Orders requested: 100,000
- Customers: 1,000
- Hot-key share: 65%
- Hot customer: `C000001`
- Salt buckets: 16
- Seed: 42
- Input rows including edge cases: 100,006

## Results

Source: `outputs/run_id=linux-demo-001/performance_comparison.json`

Three repetitions were performed for each join strategy.

| Metric | Baseline | Salted |
|---|---:|---:|
| Median runtime (seconds) | 0.6547 | 0.9071 |
| Order count | 100,000 | 100,000 |
| Revenue | 25,097,777.04 | 25,097,777.04 |

Individual runtime samples:

- Baseline: 1.2650 s, 0.6547 s, 0.4999 s
- Salted: 3.2380 s, 0.9071 s, 0.7560 s

- Speedup (baseline / salted): 0.7218x
- Correctness equal: true
- Reconciliation passed: true
- Accepted rows: 100,000
- Rejected rows: 6
- Accepted event IDs unique: true
- Accepted amounts nonnegative: true
- Accepted customer references valid: true
- End-to-end pipeline duration: 68.642 seconds

## Interpretation

Salting did not improve median runtime in this local benchmark. The salted
join took 0.9071 seconds compared with 0.6547 seconds for the baseline,
making it approximately 38.5% slower (0.7218x speedup).

The additional salting and redistribution work introduces overhead. On this
dataset and local environment, that overhead outweighed any benefit from
distributing the hot key. The result demonstrates that salting is a
workload-dependent optimization and should be applied only when measured
skew-related costs justify it.

Both strategies produced identical aggregate results, and the pipeline's
data-quality reconciliation passed: 100,006 input rows were accounted for
as 100,000 accepted and 6 rejected.

Spark UI task-level skew observations were not recorded for this run, so no
claims about individual task durations or shuffle imbalance are made.

## Reproduction

```bash
python -m skewshield.cli run \
  --input data/generated \
  --output outputs \
  --run-id linux-demo-001 \
  --salt-buckets 16 \
  --repetitions 3 \
  --master 'local[*]' \
  --shuffle-partitions 8 \
  --aqe
```
