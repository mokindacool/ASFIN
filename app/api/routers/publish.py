from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.schemas import PublishResponse, PublishedVersionOut
from app.services.publish_service import PublishService

router = APIRouter(tags=["publish"])


@router.post("/api/v1/ingestions/{ingestion_id}/publish", response_model=PublishResponse)
def publish_ingestion(ingestion_id: int, db: Session = Depends(get_db)):
    result = PublishService(db).publish(ingestion_id)
    return {
        "version": result.version,
        "table_name": result.table_name,
        "row_count": result.row_count,
    }


@router.get("/api/v1/datasets/{dataset_id}/versions", response_model=List[PublishedVersionOut])
def list_versions(dataset_id: int, db: Session = Depends(get_db)):
    return PublishService(db).list_versions(dataset_id)


@router.get("/api/v1/versions/{version_id}", response_model=PublishedVersionOut)
def get_version(version_id: int, db: Session = Depends(get_db)):
    return PublishService(db).get_version(version_id)
