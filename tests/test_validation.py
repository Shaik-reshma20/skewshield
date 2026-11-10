import json
from pathlib import Path
from skewshield.validation import load_customers, load_and_validate_orders


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(x) + "\n" for x in rows))


def test_validation_quarantines_null_negative_orphan_duplicate_and_malformed(spark, tmp_path):
    write_jsonl(tmp_path / "customers.jsonl", [
        {"customer_id": "C1", "customer_name": "One", "segment": "consumer"}
    ])
    orders = [
        {"event_id": "E1", "customer_id": "C1", "event_ts": "2025-01-01T00:00:00Z",
         "amount": 12.5, "status": "paid"},
        {"event_id": "E1", "customer_id": "C1", "event_ts": "2025-01-01T00:00:00Z",
         "amount": 12.5, "status": "paid"},
        {"event_id": "E2", "customer_id": "C1", "event_ts": "2025-01-01T00:00:00Z",
         "amount": None, "status": "paid"},
        {"event_id": "E3", "customer_id": "C1", "event_ts": "2025-01-01T00:00:00Z",
         "amount": -2, "status": "paid"},
        {"event_id": "E4", "customer_id": "UNKNOWN", "event_ts": "2025-01-01T00:00:00Z",
         "amount": 2, "status": "paid"},
        {"event_id": "E5", "customer_id": "C1", "event_ts": "2025-01-01T00:00:00Z",
         "amount": 2, "status": "paid", "new_column": "unexpected"},
    ]
    path = tmp_path / "orders.jsonl"
    write_jsonl(path, orders)
    with path.open("a") as f:
        f.write('{"broken":\n')

    customers = load_customers(spark, str(tmp_path / "customers.jsonl"))
    accepted, rejects = load_and_validate_orders(
        spark, str(path), customers, "test-run"
    )
    assert accepted.count() == 1
    assert rejects.count() == 6
    reasons = {r["reject_reason"] for r in rejects.select("reject_reason").collect()}
    assert "duplicate_event_id" in reasons
    assert "invalid_customer_reference" in reasons
    assert "malformed_json" in reasons
    assert "unexpected_schema_field" in reasons
    assert any("negative_amount" in reason for reason in reasons)
    assert any("missing_amount" in reason for reason in reasons)


def test_empty_batch(spark, tmp_path):
    (tmp_path / "customers.jsonl").write_text(
        '{"customer_id":"C1","customer_name":"One","segment":"consumer"}\n'
    )
    (tmp_path / "orders.jsonl").write_text("")
    customers = load_customers(spark, str(tmp_path / "customers.jsonl"))
    accepted, rejects = load_and_validate_orders(
        spark, str(tmp_path / "orders.jsonl"), customers, "empty"
    )
    assert accepted.count() == 0
    assert rejects.count() == 0
