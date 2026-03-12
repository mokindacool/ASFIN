from fastapi import APIRouter, Depends, FastAPI, HTTPException
from sqlalchemy import Session
from pydantic import BaseModel 
from typing import Optional  
from uuid import UUID
from api.dependencies import get_db
from services.dataset_service import DatasetService

class DatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    owner: str

class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None

router = APIRouter() # collects routes
service = DatasetService() 

@router.post("/api/router/datasets")
def create_dataset(body: DatasetCreate, db: Session = Depends(get_db)):
    return service.create(db, body)

@router.get("/api/router/datasets/{id}")
def get_dataset(id: UUID, db: Session = Depends(get_db)):
    dataset = service.get(db, id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset

@router.get("/api/router/datasets")
def get_datasets(db: Session = Depends(get_db)):
    return service.list(db)

@router.patch("/api/router/datasets/{id}")
def update_dataset(dataset_id: UUID, body: DatasetUpdate, db: Session = Depends(get_db)):
    dataset = service.update(db, dataset_id, body)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset

@router.delete("/api/v1/datasets/{id}")
def delete_dataset(dataset_id: UUID, db: Session = Depends(get_db)):
    deleted = service.delete(db, dataset_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Dataset not found")