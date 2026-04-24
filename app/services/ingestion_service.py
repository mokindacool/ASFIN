import hashlib
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.core.models import Dataset, Ingestion
from app.services.storage import StorageService


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    async def create(self, dataset_id: int, file: UploadFile) -> Ingestion:
        dataset = (
            self.db.query(Dataset)
            .filter(Dataset.id == dataset_id, Dataset.is_deleted == False)  # noqa: E712
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        content = await file.read()
        sha256 = hashlib.sha256(content).hexdigest()

        duplicate = (
            self.db.query(Ingestion)
            .filter(
                Ingestion.dataset_id == dataset_id,
                Ingestion.file_sha256 == sha256,
            )
            .first()
        )
        if duplicate:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Duplicate file: ingestion {duplicate.id} already has this "
                    f"content (sha256={sha256[:12]}...)"
                ),
            )

        filename = file.filename or "upload"
        ext = Path(filename).suffix.lower() or ".bin"

        ingestion = Ingestion(
            dataset_id=dataset_id,
            status="pending",
            original_filename=filename,
            file_ext=ext,
            file_size_bytes=len(content),
            file_sha256=sha256,
        )
        self.db.add(ingestion)
        self.db.commit()
        self.db.refresh(ingestion)

        StorageService.ensure_raw_dir(dataset_id, ingestion.id)
        raw_path = StorageService.raw_path(dataset_id, ingestion.id, ext)
        raw_path.write_bytes(content)

        ingestion.raw_path = str(raw_path)
        self.db.commit()
        self.db.refresh(ingestion)

        return ingestion

    def get(self, ingestion_id: int) -> Ingestion:
        ingestion = self.db.query(Ingestion).filter(Ingestion.id == ingestion_id).first()
        if not ingestion:
            raise HTTPException(status_code=404, detail="Ingestion not found")
        return ingestion

    def list_for_dataset(
        self, dataset_id: int, page: int = 1, page_size: int = 20
    ) -> tuple[list[Ingestion], int]:
        dataset = (
            self.db.query(Dataset)
            .filter(Dataset.id == dataset_id, Dataset.is_deleted == False)  # noqa: E712
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        query = self.db.query(Ingestion).filter(Ingestion.dataset_id == dataset_id)
        total = query.count()
        items = (
            query.order_by(Ingestion.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        return items, total

    def preview(
        self,
        ingestion_id: int,
        rows: int = 50,
        zone: Literal["raw", "clean"] = "raw",
    ) -> list[dict[str, Any]]:
        ingestion = self.get(ingestion_id)
        max_rows = max(1, rows)

        if zone == "raw":
            preview_path = Path(ingestion.raw_path or "")
            if not preview_path.exists():
                raise HTTPException(status_code=404, detail="Raw file not found")
            return self._preview_raw_file(preview_path, max_rows)

        clean_root = Path(ingestion.clean_path or "")
        preview_path = self._resolve_clean_preview_path(clean_root)
        return self._dataframe_to_records(pd.read_parquet(preview_path).head(max_rows))

    def _preview_raw_file(self, path: Path, rows: int) -> list[dict[str, Any]]:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return self._dataframe_to_records(pd.read_csv(path, nrows=rows))
        if suffix == ".txt":
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()[:rows]
            return [
                {"line_number": index + 1, "content": line}
                for index, line in enumerate(lines)
            ]
        raise HTTPException(
            status_code=400,
            detail=f"Preview is not supported for raw file type '{suffix or 'unknown'}'",
        )

    def _resolve_clean_preview_path(self, clean_root: Path) -> Path:
        if not clean_root.exists():
            raise HTTPException(status_code=404, detail="Clean output not found")
        if clean_root.is_file():
            if clean_root.suffix.lower() != ".parquet":
                raise HTTPException(status_code=400, detail="Clean preview expects Parquet output")
            return clean_root

        parquet_files = sorted(clean_root.glob("*.parquet"))
        if not parquet_files:
            raise HTTPException(status_code=404, detail="No Parquet files found in clean output")
        return parquet_files[0]

    def _dataframe_to_records(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        sanitized = df.astype(object).where(pd.notna(df), None)
        records = sanitized.to_dict(orient="records")

        for record in records:
            for key, value in list(record.items()):
                if hasattr(value, "isoformat") and not isinstance(value, str):
                    record[key] = value.isoformat()

        return records
