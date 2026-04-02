from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class DatasetCreate(BaseModel):
    name: str
    process_type: str
    description: Optional[str] = None


class DatasetUpdate(BaseModel):
    description: Optional[str] = None


class DatasetOut(BaseModel):
    id: int
    name: str
    process_type: str
    description: Optional[str]
    created_at: datetime
    is_deleted: bool

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


class IngestionOut(BaseModel):
    id: int
    dataset_id: int
    status: str
    original_filename: str
    file_ext: str
    raw_path: Optional[str]
    file_size_bytes: Optional[int]
    file_sha256: Optional[str]
    error_message: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}


class IngestionList(BaseModel):
    items: List[IngestionOut]
    total: int
    page: int
    page_size: int
