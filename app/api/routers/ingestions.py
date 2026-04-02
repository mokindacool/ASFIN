from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.schemas import IngestionList, IngestionOut
from app.services.ingestion_service import IngestionService

router = APIRouter(tags=["ingestions"])


@router.post(
    "/api/v1/datasets/{dataset_id}/ingestions",
    response_model=IngestionOut,
    status_code=201,
)
async def create_ingestion(
    dataset_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
    Upload a file to start a new ingestion pipeline run.
    Returns 409 if the identical file (same SHA-256) was already uploaded for this dataset.
    """
    return await IngestionService(db).create(dataset_id, file)


@router.get("/api/v1/ingestions/{ingestion_id}", response_model=IngestionOut)
def get_ingestion(ingestion_id: int, db: Session = Depends(get_db)):
    """Poll the status and metadata of a single ingestion."""
    return IngestionService(db).get(ingestion_id)


@router.get(
    "/api/v1/datasets/{dataset_id}/ingestions",
    response_model=IngestionList,
)
def list_ingestions(
    dataset_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """List ingestions for a dataset, newest first, paginated."""
    items, total = IngestionService(db).list_for_dataset(dataset_id, page, page_size)
    return IngestionList(items=items, total=total, page=page, page_size=page_size)
