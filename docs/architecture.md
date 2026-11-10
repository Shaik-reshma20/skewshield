# Architecture and Design

```text
generator.py
  -> customers.jsonl + orders.jsonl
  -> Spark text reader + from_json
  -> schema/domain validation
       -> quarantine_orders
       -> deduplicate event_id
       -> FK validation
  -> accepted fact_orders
       -> standard join -> aggregate
       -> salted hot-key join -> aggregate
  -> Parquet targets + JSON reports
```

## Why salting?

Hash partitioning by a highly frequent join key can send a disproportionate
amount of fact data to one reducer. Salting adds a deterministic bucket to the
hot fact key. The corresponding hot dimension row is expanded to all salt
values, while non-hot dimension rows use salt zero. This spreads the hot key
across multiple composite join keys without changing the logical join result.

## Trade-offs

- Salting adds a small dimension-side replication and an extra join column.
- Too few buckets leave residual skew; too many add scheduling/shuffle overhead.
- AQE can mitigate skew automatically, so compare both strategies with AQE
  enabled and disabled.
- The implementation targets one known hot key for clarity. A production
  implementation should discover multiple hot keys from observed key
  frequencies and choose a threshold based on workload evidence.
