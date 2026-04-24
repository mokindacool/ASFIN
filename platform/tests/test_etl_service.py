import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd


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


storage_module = load_module("asfin_platform.services.storage", "services/storage.py")
load_module("asfin_platform.core.config", "core/config.py")
load_module("asfin_platform.core.database", "core/database.py")
load_module("asfin_platform.core.models", "core/models.py")
elt_service_module = load_module("asfin_platform.services.elt_service", "services/elt_service.py")

ETLService = elt_service_module.ETLService


class FakeSession:
    def __init__(self, ingestion, dataset):
        self.ingestion = ingestion
        self.dataset = dataset
        self.scalar_calls = 0
        self.commits = 0

    def scalar(self, stmt):
        self.scalar_calls += 1
        if self.scalar_calls == 1:
            return self.ingestion
        if self.scalar_calls == 2:
            return self.dataset
        return None

    def commit(self):
        self.commits += 1


def test_etl_service_stages_processes_writes_parquet_and_cleans_staging(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_module, "DATA_ROOT", tmp_path)

    dataset_id = uuid4()
    ingestion_id = uuid4()
    raw_dir = tmp_path / "raw" / str(dataset_id) / str(ingestion_id)
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / "original.csv"
    raw_path.write_text("amount\n1\n2\n", encoding="utf-8")

    dataset = SimpleNamespace(id=dataset_id, process_type="ABSA")
    ingestion = SimpleNamespace(
        id=ingestion_id,
        dataset_id=dataset_id,
        original_filename="source.csv",
        file_ext=".csv",
        raw_path=str(raw_path),
        clean_path=None,
        row_count_clean=None,
        status="pending",
        error_message=None,
        completed_at=None,
    )
    db = FakeSession(ingestion, dataset)

    def fake_get_pfuncs(process_type, func):
        assert process_type == "ABSA"
        assert func == "pull"

        def fake_pull(path, _process_type):
            staged = Path(path) / "source.csv"
            assert staged.exists()
            return {"source": pd.DataFrame({"amount": [1, 2]})}

        return fake_pull

    class FakeProcessor:
        def __init__(self, process_type):
            assert process_type == "ABSA"

        def dispatch(self, dfs, names, reporting=False):
            assert len(dfs) == 1
            assert names == ["source"]
            return [pd.DataFrame({"amount": [10, 20]})], ["ABSA"]

    monkeypatch.setattr(elt_service_module, "get_pFuncs", fake_get_pfuncs)
    monkeypatch.setattr(elt_service_module, "ASUCProcessor", FakeProcessor)

    output_paths = ETLService(db).run(ingestion_id)

    assert ingestion.status == "clean_ready"
    assert ingestion.row_count_clean == 2
    assert ingestion.clean_path == str(tmp_path / "clean" / str(dataset_id) / str(ingestion_id))
    assert len(output_paths) == 1

    output_path = Path(output_paths[0])
    assert output_path.exists()
    assert output_path.suffix == ".parquet"

    written = pd.read_parquet(output_path)
    assert written["amount"].tolist() == [10, 20]

    staging_dir = tmp_path / "staging" / str(ingestion_id)
    assert not staging_dir.exists()
