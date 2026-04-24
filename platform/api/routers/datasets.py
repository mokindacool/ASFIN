from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..dependencies import get_db
from ...core.schemas import DatasetCreate, DatasetResponse, DatasetUpdate
from ...services.dataset_service import DatasetService


router = APIRouter(prefix="/api/v1/datasets", tags=["datasets"])


@router.post("", response_model=DatasetResponse)
def create_dataset(body: DatasetCreate, db: Session = Depends(get_db)):
    service = DatasetService()
    return service.create(db, body)


@router.get("/{dataset_id}", response_model=DatasetResponse)
def get_dataset(dataset_id: UUID, db: Session = Depends(get_db)):
    service = DatasetService()
    dataset = service.get(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.patch("/{dataset_id}", response_model=DatasetResponse)
def update_dataset(dataset_id: UUID, body: DatasetUpdate, db: Session = Depends(get_db)):
    service = DatasetService()
    dataset = service.update(db, dataset_id, body)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset
