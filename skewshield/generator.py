import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_dataset(output_dir, orders=100_000, customers=1_000, seed=42,
                     skew_fraction=0.65, hot_customer_id="C000001",
                     include_edge_cases=True):
    """Write reproducible JSONL source files. Invalid examples are intentional."""
    if customers < 2:
        raise ValueError("customers must be at least 2")
    if orders < 1:
        raise ValueError("orders must be positive")
    if not 0 <= skew_fraction <= 1:
        raise ValueError("skew_fraction must be between 0 and 1")

    rng = random.Random(seed)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    customer_rows = []
    for i in range(1, customers + 1):
        cid = f"C{i:06d}"
        customer_rows.append({
            "customer_id": cid,
            "customer_name": f"Customer {i:06d}",
            "segment": ["consumer", "business", "enterprise"][i % 3],
        })
    if hot_customer_id not in {r["customer_id"] for r in customer_rows}:
        hot_customer_id = customer_rows[0]["customer_id"]

    with (out / "customers.jsonl").open("w", encoding="utf-8") as f:
        for row in customer_rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")

    normal_ids = [r["customer_id"] for r in customer_rows
                  if r["customer_id"] != hot_customer_id]
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    order_rows = []
    for i in range(orders):
        if rng.random() < skew_fraction:
            cid = hot_customer_id
        else:
            cid = rng.choice(normal_ids)
        ts = start + timedelta(seconds=rng.randint(0, 60 * 60 * 24 * 90))
        order_rows.append({
            "event_id": f"E{i + 1:012d}",
            "customer_id": cid,
            "event_ts": ts.isoformat().replace("+00:00", "Z"),
            "amount": round(rng.uniform(1, 500), 2),
            "status": rng.choice(["created", "paid", "shipped", "cancelled"]),
        })

    with (out / "orders.jsonl").open("w", encoding="utf-8") as f:
        for row in order_rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
        if include_edge_cases:
            # Exact duplicate event: pipeline must retain one deterministic copy.
            f.write(json.dumps(order_rows[0], separators=(",", ":")) + "\n")
            # Missing required field.
            missing = dict(order_rows[1])
            missing["amount"] = None
            f.write(json.dumps(missing, separators=(",", ":")) + "\n")
            # Domain-rule violation.
            negative = dict(order_rows[2])
            negative["event_id"] = "E_INVALID_NEGATIVE"
            negative["amount"] = -10.0
            f.write(json.dumps(negative, separators=(",", ":")) + "\n")
            # Invalid foreign key.
            orphan = dict(order_rows[3])
            orphan["event_id"] = "E_INVALID_ORPHAN"
            orphan["customer_id"] = "C999999"
            f.write(json.dumps(orphan, separators=(",", ":")) + "\n")
            # Unexpected field simulates an upstream schema change.
            schema_drift = dict(order_rows[4])
            schema_drift["event_id"] = "E_SCHEMA_DRIFT"
            schema_drift["unexpected_new_column"] = "new-value"
            f.write(json.dumps(schema_drift, separators=(",", ":")) + "\n")
            # Malformed JSON line.
            f.write('{"event_id": "E_MALFORMED", "amount":\n')

    return {
        "output_dir": str(out.resolve()),
        "requested_orders": orders,
        "customers": customers,
        "hot_customer_id": hot_customer_id,
        "seed": seed,
        "skew_fraction": skew_fraction,
        "edge_cases_included": include_edge_cases,
    }
