import asyncio
import hashlib
import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session


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
    spec = importlib.util.spec_from_file_location(
        module_name,
        PLATFORM_ROOT / relative_path,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


config_module = load_module("asfin_platform.core.config", "core/config.py")
database_module = load_module("asfin_platform.core.database", "core/database.py")
storage_module = load_module("asfin_platform.services.storage", "services/storage.py")
load_module("asfin_platform.core.models", "core/models.py")
ingestion_service_module = load_module(
    "asfin_platform.services.ingestion_service",
    "services/ingestion_service.py",
)

settings = config_module.settings
get_db = database_module.get_db
StorageService = storage_module.StorageService
IngestionService = ingestion_service_module.IngestionService


class FakeUploadFile:
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self._content = content

    async def read(self) -> bytes:
        return self._content


class FakeSession:
    def __init__(self, dataset=None, duplicate=None):
        self.dataset = dataset
        self.duplicate = duplicate
        self.scalar_calls = 0
        self.added = []
        self.committed = False
        self.refreshed = False

    def scalar(self, stmt):
        self.scalar_calls += 1
        if self.scalar_calls == 1:
            return self.dataset
        if self.scalar_calls == 2:
            return self.duplicate
        return None

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        ingestion = self.added[-1]
        ingestion.id = uuid4()
        ingestion.created_at = datetime.now(timezone.utc)

    def commit(self):
        self.committed = True

    def refresh(self, obj):
        self.refreshed = True


def run(coro):
    return asyncio.run(coro)


def test_settings_and_get_db_are_available():
    assert isinstance(settings.database_url, str)

    db_gen = get_db()
    session = next(db_gen)
    assert isinstance(session, Session)
    db_gen.close()


def test_storage_service_builds_and_writes_canonical_raw_path(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_module, "DATA_ROOT", tmp_path)

    dataset_id = str(uuid4())
    ingestion_id = str(uuid4())

    expected = tmp_path / "raw" / dataset_id / ingestion_id / "original.csv"
    assert StorageService.raw_path(dataset_id, ingestion_id, ".csv") == expected

    written = StorageService.write_raw_bytes(dataset_id, ingestion_id, "csv", b"a,b\n1,2\n")
    assert written == expected
    assert written.exists()
    assert written.read_bytes() == b"a,b\n1,2\n"


def test_ingestion_service_create_stores_pending_ingestion_and_returns_id(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_module, "DATA_ROOT", tmp_path)

    dataset = SimpleNamespace(id=uuid4(), is_active=True)
    db = FakeSession(dataset=dataset, duplicate=None)
    service = IngestionService(db)
    content = b"date,amount\n2024-01-01,100\n"

    ingestion_id = run(service.create(dataset.id, FakeUploadFile("report.csv", content)))

    assert ingestion_id == db.added[0].id
    assert db.added[0].status == "pending"
    assert db.added[0].file_sha256 == hashlib.sha256(content).hexdigest()
    assert db.added[0].raw_path.endswith("original.csv")
    assert db.committed is True
    assert db.refreshed is True

    raw_path = tmp_path / "raw" / str(dataset.id) / str(ingestion_id) / "original.csv"
    assert raw_path.exists()
    assert raw_path.read_bytes() == content


def test_ingestion_service_rejects_duplicate_upload():
    dataset = SimpleNamespace(id=uuid4(), is_active=True)
    duplicate = SimpleNamespace(id=uuid4())
    db = FakeSession(dataset=dataset, duplicate=duplicate)
    service = IngestionService(db)

    with pytest.raises(HTTPException) as exc:
        run(service.create(dataset.id, FakeUploadFile("report.csv", b"same-content")))

    assert exc.value.status_code == 409
