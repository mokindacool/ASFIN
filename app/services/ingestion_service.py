import hashlib
from pathlib import Path

from fastapi import HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.core.models import Dataset, Ingestion
from app.services.storage import StorageService


class IngestionService:
    def __init__(self, db: Session):
        self.db = db

    async def create(self, dataset_id: int, file: UploadFile) -> Ingestion:
        # 1. Verify the dataset exists
        dataset = (
            self.db.query(Dataset)
            .filter(Dataset.id == dataset_id, Dataset.is_deleted == False)  # noqa: E712
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # 2. Buffer the full file and compute SHA-256
        content = await file.read()
        sha256 = hashlib.sha256(content).hexdigest()

        # 3. Idempotency check — reject if this exact file was already ingested
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
                detail=f"Duplicate file: ingestion {duplicate.id} already has this content (sha256={sha256[:12]}…)",
            )

        filename = file.filename or "upload"
        ext = Path(filename).suffix.lower() or ".bin"

        # 4. Insert ingestion row (status='pending') — we need the id before writing to disk
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

        # 5. Write raw file to /data/raw/{dataset_id}/{ingestion_id}/original.{ext}
        StorageService.ensure_raw_dir(dataset_id, ingestion.id)
        raw_path = StorageService.raw_path(dataset_id, ingestion.id, ext)
        raw_path.write_bytes(content)

        ingestion.raw_path = str(raw_path)
        self.db.commit()
        self.db.refresh(ingestion)

        return ingestion

    async def create_reconcile(
        self, dataset_id: int, fr_file: UploadFile, agenda_file: UploadFile
    ) -> Ingestion:
        dataset = (
            self.db.query(Dataset)
            .filter(Dataset.id == dataset_id, Dataset.is_deleted == False)  # noqa: E712
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        fr_content = await fr_file.read()
        agenda_content = await agenda_file.read()

        sha256 = hashlib.sha256(fr_content).hexdigest()

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
                detail=f"Duplicate file: ingestion {duplicate.id} already has this content (sha256={sha256[:12]}…)",
            )

        fr_filename = fr_file.filename or "upload"
        fr_ext = Path(fr_filename).suffix.lower() or ".bin"
        agenda_filename = agenda_file.filename or "agenda"
        agenda_ext = Path(agenda_filename).suffix.lower() or ".bin"

        ingestion = Ingestion(
            dataset_id=dataset_id,
            status="pending",
            original_filename=fr_filename,
            file_ext=fr_ext,
            file_size_bytes=len(fr_content),
            file_sha256=sha256,
        )
        self.db.add(ingestion)
        self.db.commit()
        self.db.refresh(ingestion)

        StorageService.ensure_raw_dir(dataset_id, ingestion.id)
        raw_path = StorageService.raw_path(dataset_id, ingestion.id, fr_ext)
        raw_path.write_bytes(fr_content)

        secondary_path = StorageService.raw_path_secondary(dataset_id, ingestion.id, agenda_ext)
        secondary_path.write_bytes(agenda_content)

        ingestion.raw_path = str(raw_path)
        ingestion.raw_path_secondary = str(secondary_path)
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
