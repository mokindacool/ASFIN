from __future__ import annotations

import argparse
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4


PLATFORM_ROOT = Path(__file__).resolve().parent / "platform"


def ensure_test_package() -> None:
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


def default_input_for(process_type: str) -> Path:
    if process_type == "FR":
        return Path("files/input/CLEAN - FR 24_25 F1 - Cleaned_Raw.csv").resolve()
    raise ValueError(f"No default sample is configured for process type '{process_type}'")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a smoke test through the new platform ETL service.")
    parser.add_argument("--process", default="FR", help="Dataset process type, e.g. FR or CONTINGENCY")
    parser.add_argument("--input", dest="input_path", default=None, help="Path to the raw input file")
    parser.add_argument(
        "--data-root",
        default=str((Path("data") / "smoke").resolve()),
        help="Persistent DATA_ROOT for the smoke run",
    )
    args = parser.parse_args()

    process_type = args.process.upper()
    sample = Path(args.input_path).resolve() if args.input_path else default_input_for(process_type)
    if not sample.exists():
        raise SystemExit(f"Input file not found: {sample}")

    storage_module = load_module("asfin_platform.services.storage", "services/storage.py")
    load_module("asfin_platform.core.config", "core/config.py")
    load_module("asfin_platform.core.database", "core/database.py")
    load_module("asfin_platform.core.models", "core/models.py")
    elt_service_module = load_module("asfin_platform.services.elt_service", "services/elt_service.py")
    ETLService = elt_service_module.ETLService

    data_root = Path(args.data_root).resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    storage_module.DATA_ROOT = data_root

    dataset_id = uuid4()
    ingestion_id = uuid4()
    ext = sample.suffix or ".bin"

    raw_dir = data_root / "raw" / str(dataset_id) / str(ingestion_id)
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"original{ext}"
    raw_path.write_bytes(sample.read_bytes())

    dataset = SimpleNamespace(id=dataset_id, process_type=process_type)
    ingestion = SimpleNamespace(
        id=ingestion_id,
        dataset_id=dataset_id,
        original_filename=sample.name,
        file_ext=ext,
        raw_path=str(raw_path),
        clean_path=None,
        row_count_clean=None,
        status="pending",
        error_message=None,
        completed_at=None,
    )
    db = FakeSession(ingestion, dataset)

    try:
        output_paths = ETLService(db).run(ingestion_id)
    except Exception as exc:
        print("RESULT: FAILURE")
        print(f"process_type={process_type}")
        print(f"input={sample}")
        print(f"data_root={data_root}")
        print(f"status={ingestion.status}")
        print(f"error_message={ingestion.error_message}")
        print(f"exception_type={type(exc).__name__}")
        print(f"exception={exc}")
        return 1

    print("RESULT: SUCCESS")
    print(f"process_type={process_type}")
    print(f"input={sample}")
    print(f"data_root={data_root}")
    print(f"status={ingestion.status}")
    print(f"clean_path={ingestion.clean_path}")
    print(f"row_count_clean={ingestion.row_count_clean}")
    print(f"outputs={output_paths}")
    for out in output_paths:
        print(f"exists={Path(out).exists()} path={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
