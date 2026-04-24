from __future__ import annotations

import os
from pathlib import Path


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data"))


class StorageService:
    """
    Centralizes raw/clean storage path construction behind one class.

    If the backing store changes later (for example to S3), callers can keep
    using this interface while only this implementation changes.
    """

    @staticmethod
    def raw_dir(dataset_id: str, ingestion_id: str) -> Path:
        return DATA_ROOT / "raw" / str(dataset_id) / str(ingestion_id)

    @classmethod
    def raw_path(cls, dataset_id: str, ingestion_id: str, ext: str) -> Path:
        """
        Canonical raw object path:
        /data/raw/{dataset_id}/{ingestion_id}/original.{ext}
        """
        normalized_ext = ext if str(ext).startswith(".") else f".{ext}"
        return cls.raw_dir(dataset_id, ingestion_id) / f"original{normalized_ext}"

    @staticmethod
    def clean_dir(dataset_id: str, ingestion_id: str) -> Path:
        return DATA_ROOT / "clean" / str(dataset_id) / str(ingestion_id)

    @staticmethod
    def staging_dir(ingestion_id: str) -> Path:
        return DATA_ROOT / "staging" / str(ingestion_id)

    @classmethod
    def ensure_raw_dir(cls, dataset_id: str, ingestion_id: str) -> Path:
        path = cls.raw_dir(dataset_id, ingestion_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def ensure_clean_dir(cls, dataset_id: str, ingestion_id: str) -> Path:
        path = cls.clean_dir(dataset_id, ingestion_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def ensure_staging_dir(cls, ingestion_id: str) -> Path:
        path = cls.staging_dir(ingestion_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def write_raw_bytes(
        cls,
        dataset_id: str,
        ingestion_id: str,
        ext: str,
        content: bytes,
    ) -> Path:
        """
        Persist raw upload bytes to the canonical raw-zone location and
        return the written path.
        """
        cls.ensure_raw_dir(dataset_id, ingestion_id)
        dest = cls.raw_path(dataset_id, ingestion_id, ext)
        dest.write_bytes(content)
        return dest

    @staticmethod
    def resolve_path(stored_path: str | Path) -> Path:
        return Path(stored_path)
