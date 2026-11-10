# Data Dictionary

## Source entities

### `customers.jsonl` — customer dimension

**Grain:** one row per customer.

| Column | Type | Nullable | Rule |
|---|---|---:|---|
| customer_id | string | no | Stable synthetic customer key; unique |
| customer_name | string | yes | Display name |
| segment | string | yes | consumer, business, enterprise |

### `orders.jsonl` — order event source

**Grain:** one source event per line. Duplicate event IDs are possible by design.

| Column | Type | Nullable | Rule |
|---|---|---:|---|
| event_id | string | no | Stable event primary key |
| customer_id | string | no | FK to customers.customer_id |
| event_ts | ISO-8601 timestamp | no | Event time, UTC |
| amount | double | no | Must be >= 0 |
| status | string | no | Non-empty status |

## Target entities

### `fact_orders`

One accepted row per `event_id`; preserves event time and source lineage.
Includes `source_file` and `run_id`.

### `dim_customers`

One row per `customer_id`.

### `quarantine_orders`

One row per rejected source record. Contains `raw_record`, `source_file`,
`reject_reason`, and `run_id`. Multiple reasons are pipe-separated for
schema/domain failures. Duplicate and FK failures have explicit reasons.

## Relationships and assumptions

- `fact_orders.customer_id` references `dim_customers.customer_id`.
- `fact_orders.event_id` is unique after deduplication.
- Amount is a non-negative monetary-like synthetic measure, represented as
  double for benchmark simplicity. A production finance pipeline should use
  DecimalType with an explicit currency and scale.
- Timestamps are parsed in UTC.
- Late events are valid and retained; no event-time watermark is applied.
- The generated hot customer is the intended skew key.
- The source is immutable JSONL for the duration of a run.
