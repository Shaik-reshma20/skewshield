import json
from skewshield.generator import generate_dataset


def test_generator_is_reproducible(tmp_path):
    a = tmp_path / "a"
    b = tmp_path / "b"
    generate_dataset(a, orders=20, customers=5, seed=7, include_edge_cases=False)
    generate_dataset(b, orders=20, customers=5, seed=7, include_edge_cases=False)
    assert (a / "orders.jsonl").read_text() == (b / "orders.jsonl").read_text()
    assert (a / "customers.jsonl").read_text() == (b / "customers.jsonl").read_text()


def test_generator_includes_negative_scenarios(tmp_path):
    out = tmp_path / "sample"
    generate_dataset(out, orders=10, customers=5, include_edge_cases=True)
    lines = (out / "orders.jsonl").read_text().splitlines()
    assert len(lines) == 16
    assert '"E_MALFORMED"' in lines[-1]
    assert any("unexpected_new_column" in line for line in lines)
