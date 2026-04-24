from __future__ import annotations

import datetime as dt
import re
import shutil
from pathlib import Path
from uuid import UUID

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ASFINT.Config.Config import get_pFuncs
from ASFINT.Transform.Processor import ASUCProcessor

from ..core.models import Dataset, Ingestion
from .storage import StorageService


_SAFE_NAME = re.compile(r'[\\/:"*?<>|]+')


class ETLService:
    def __init__(self, db: Session):
        self.db = db

    def run(self, ingestion_id: UUID | str) -> list[str]:
        ingestion = self.db.scalar(select(Ingestion).where(Ingestion.id == ingestion_id))
        if not ingestion:
            raise HTTPException(status_code=404, detail="Ingestion not found")

        dataset = self.db.scalar(select(Dataset).where(Dataset.id == ingestion.dataset_id))
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")

        process_type = dataset.process_type.upper()
        raw_path = Path(ingestion.raw_path)
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}")

        staging_dir = StorageService.ensure_staging_dir(str(ingestion.id))
        clean_dir = StorageService.ensure_clean_dir(str(dataset.id), str(ingestion.id))

        ingestion.status = "processing"
        self.db.commit()

        try:
            staged_path = self._stage_raw_file(process_type, ingestion, raw_path, staging_dir)

            puller = get_pFuncs(process_type, "pull")
            pulled = puller(str(staging_dir), process_type)

            processor = ASUCProcessor(process_type)
            clean_dfs, clean_names = processor.dispatch(
                list(pulled.values()),
                list(pulled.keys()),
                reporting=False,
            )

            output_paths: list[str] = []
            total_rows = 0
            for name, df in zip(clean_names, clean_dfs):
                safe_name = self._safe_output_name(name)
                out_path = clean_dir / f"{safe_name}.parquet"
                df.to_parquet(out_path, index=False)
                output_paths.append(str(out_path))
                total_rows += int(len(df))

            ingestion.clean_path = str(clean_dir)
            ingestion.row_count_clean = total_rows
            ingestion.status = "clean_ready"
            ingestion.completed_at = dt.datetime.now(dt.timezone.utc)
            self.db.commit()

            return output_paths
        except Exception as exc:
            ingestion.status = "failed"
            ingestion.error_message = str(exc)
            ingestion.completed_at = dt.datetime.now(dt.timezone.utc)
            self.db.commit()
            raise
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _stage_raw_file(
        self,
        process_type: str,
        ingestion: Ingestion,
        raw_path: Path,
        staging_dir: Path,
    ) -> Path:
        staged_name = self._staged_filename(process_type, ingestion.original_filename, ingestion.file_ext)
        staged_path = staging_dir / staged_name
        shutil.copy2(raw_path, staged_path)
        return staged_path

    def _staged_filename(self, process_type: str, original_filename: str, file_ext: str) -> str:
        ext = file_ext if str(file_ext).startswith(".") else f".{file_ext}"
        stem = Path(original_filename).stem
        lower_stem = stem.lower()

        if process_type == "RECONCILE":
            if "agenda" in lower_stem:
                return f"{stem}{ext}"
            if "cleaned" in lower_stem:
                return f"{stem}{ext}"
            return f"{stem} Cleaned{ext}"

        if process_type == "CONTINGENCY" and "agenda" not in lower_stem:
            return f"{stem} Agenda{ext}"

        return f"{stem}{ext}"

    def _safe_output_name(self, name: str) -> str:
        return _SAFE_NAME.sub("-", str(name)).strip()
