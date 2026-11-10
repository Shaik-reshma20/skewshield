from skewshield.schemas import TARGET_SCHEMA_DESCRIPTION


def test_data_dictionary_has_expected_entities():
    assert set(TARGET_SCHEMA_DESCRIPTION) == {
        "fact_orders", "dim_customers", "quarantine_orders"
    }
    assert TARGET_SCHEMA_DESCRIPTION["fact_orders"]["primary_key"] == ["event_id"]
