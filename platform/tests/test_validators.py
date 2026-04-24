import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd


PLATFORM_ROOT = Path(__file__).resolve().parents[1]


def ensure_test_package():
    if "asfin_platform" in sys.modules:
        return

    root_pkg = types.ModuleType("asfin_platform")
    root_pkg.__path__ = [str(PLATFORM_ROOT)]
    sys.modules["asfin_platform"] = root_pkg

    validators_pkg = types.ModuleType("asfin_platform.validators")
    validators_pkg.__path__ = [str(PLATFORM_ROOT / "validators")]
    sys.modules["asfin_platform.validators"] = validators_pkg


def load_module(module_name: str, relative_path: str):
    ensure_test_package()
    spec = importlib.util.spec_from_file_location(module_name, PLATFORM_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


load_module("asfin_platform.validators.base", "validators/base.py")
schema_validator_module = load_module(
    "asfin_platform.validators.schema_validator",
    "validators/schema_validator.py",
)
join_validator_module = load_module(
    "asfin_platform.validators.join_validator",
    "validators/join_validator.py",
)

SchemaValidator = schema_validator_module.SchemaValidator
JoinValidator = join_validator_module.JoinValidator


def test_schema_validator_passes_when_columns_exist_and_values_are_coercible():
    df = pd.DataFrame(
        {
            "amount": ["10", "20", "30"],
            "posted_at": ["2026-04-01", "2026-04-02", "2026-04-03"],
        }
    )
    config = {
        "schema_def": [
            {"name": "amount", "dtype": "int"},
            {"name": "posted_at", "dtype": "datetime"},
        ]
    }

    result = SchemaValidator().run(df, config)

    assert result.check_name == "schema"
    assert result.status == "pass"
    assert result.details["validated_columns"] == ["amount", "posted_at"]


def test_schema_validator_fails_on_missing_required_columns():
    df = pd.DataFrame({"amount": ["10", "20"]})
    config = {
        "schema_def": [
            {"name": "amount", "dtype": "int"},
            {"name": "posted_at", "dtype": "datetime"},
        ]
    }

    result = SchemaValidator().run(df, config)

    assert result.status == "fail"
    assert result.details["missing_columns"] == ["posted_at"]


def test_schema_validator_fails_when_values_are_not_coercible():
    df = pd.DataFrame({"amount": ["10", "bad-value", "30"]})
    config = {"schema_def": [{"name": "amount", "dtype": "int"}]}

    result = SchemaValidator().run(df, config)

    assert result.status == "fail"
    assert result.details["type_errors"][0]["column"] == "amount"
    assert result.details["type_errors"][0]["invalid_count"] == 1


def test_schema_validator_skips_when_no_schema_is_provided():
    df = pd.DataFrame({"amount": ["10", "20"]})

    result = SchemaValidator().run(df, {})

    assert result.status == "skipped"
    assert result.severity == "warning"


def test_join_validator_skips_non_reconcile_datasets():
    df = pd.DataFrame({"Org Name": ["Org A", "Org B"]})

    result = JoinValidator().run(df, {"process_type": "FR"})

    assert result.status == "skipped"
    assert result.severity == "info"


def test_join_validator_fails_on_duplicate_org_name_for_reconcile():
    df = pd.DataFrame({"Org Name": ["Org A", "Org A", "Org B"]})

    result = JoinValidator().run(df, {"process_type": "RECONCILE", "input_row_count": 3})

    assert result.status == "fail"
    assert result.details["duplicate_key_count"] == 1
    assert result.details["duplicate_key_samples"] == ["Org A"]


def test_join_validator_fails_on_row_explosion_for_reconcile():
    df = pd.DataFrame({"Org Name": ["Org A", "Org B", "Org C", "Org D"]})

    result = JoinValidator().run(
        df,
        {"process_type": "RECONCILE", "input_row_count": 2, "max_row_multiplier": 1.5},
    )

    assert result.status == "fail"
    assert result.details["row_explosion_detected"] is True
    assert result.details["max_allowed_rows"] == 3


def test_join_validator_passes_reconcile_when_keys_unique_and_rows_stable():
    df = pd.DataFrame({"Org Name": ["Org A", "Org B", "Org C"]})

    result = JoinValidator().run(
        df,
        {"process_type": "RECONCILE", "input_row_count": 3, "max_row_multiplier": 1.5},
    )

    assert result.status == "pass"
    assert result.details["row_explosion_detected"] is False
