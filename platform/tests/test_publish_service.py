import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text


PLATFORM_ROOT = Path(__file__).resolve().parents[1]


def ensure_test_package():
    if "asfin_platform" in sys.modules:
        return

    root_pkg = types.ModuleType("asfin_platform")
    root_pkg.__path__ = [str(PLATFORM_ROOT)]
    sys.modules["asfin_platform"] = root_pkg

    for subpkg_name in ("core", "services"):
        subpkg = types.ModuleType(f"asfin_platform.{subpkg_name}")
        subpkg.__path__ = [str(PLATFORM_ROOT / subpkg_name)]
        sys.modules[f"asfin_platform.{subpkg_name}"] = subpkg


def load_module(module_name: str, relative_path: str):
    ensure_test_package()
    spec = importlib.util.spec_from_file_location(module_name, PLATFORM_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


load_module("asfin_platform.core.config", "core/config.py")
load_module("asfin_platform.core.database", "core/database.py")
publish_service_module = load_module("asfin_platform.services.publish_service", "services/publish_service.py")

PostgreSQLWarehousePublisher = publish_service_module.PostgreSQLWarehousePublisher


def test_postgres_publisher_sanitizes_names_and_writes_table():
    engine = create_engine("sqlite:///:memory:")
    publisher = PostgreSQLWarehousePublisher(engine=engine)

    df = pd.DataFrame(
        [["A", 100, 100], ["B", 200, 200]],
        columns=["Org Name", "Amount($)", "Amount($)"],
    )

    result = publisher.publish(df, dataset_name="Finance Committee Report", version_number=3)

    assert result.table_name == "Finance_Committee_Report_v3"
    assert result.row_count == 2
    assert "Org_Name" in result.schema_snapshot
    assert "Amount" in result.schema_snapshot

    inspector = inspect(engine)
    assert result.table_name in inspector.get_table_names()

    with engine.connect() as conn:
        rows = conn.execute(text(f'SELECT COUNT(*) FROM "{result.table_name}"')).scalar_one()
        assert rows == 2
