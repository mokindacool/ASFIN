from __future__ import annotations

import hashlib
from pathlib import Path
from uuid import UUID

from fastapi import HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..core.models import Dataset, Ingestion
from .storage import StorageService


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    async def create(
        self,
        dataset_id: UUID | str,
        file: UploadFile,
        triggered_by: str | None = None,
    ) -> UUID:
        dataset = self.db.scalar(
            select(Dataset).where(
                Dataset.id == dataset_id,
                Dataset.is_active.is_(True),
            )
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        content = await file.read()
        sha256 = hashlib.sha256(content).hexdigest()

        duplicate = self.db.scalar(
            select(Ingestion).where(
                Ingestion.dataset_id == dataset.id,
                Ingestion.file_sha256 == sha256,
                Ingestion.status.not_in(["failed", "validation_failed"]),
            )
        )
        if duplicate:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Duplicate file: ingestion {duplicate.id} already exists "
                    f"for this dataset"
                ),
            )

        filename = file.filename or "upload"
        ext = Path(filename).suffix.lower() or ".bin"

        ingestion = Ingestion(
            dataset_id=dataset.id,
            status="pending",
            original_filename=filename,
            file_ext=ext,
            raw_path="",
            file_size_bytes=len(content),
            file_sha256=sha256,
            triggered_by=triggered_by,
        )
        self.db.add(ingestion)
        self.db.flush()

        raw_path = StorageService.write_raw_bytes(
            dataset_id=str(dataset.id),
            ingestion_id=str(ingestion.id),
            ext=ext,
            content=content,
        )
        ingestion.raw_path = str(raw_path)

        self.db.commit()
        self.db.refresh(ingestion)
        return ingestion.id
