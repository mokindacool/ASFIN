
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
from app.services.ingestion_service import IngestionService
from app.core.models import get_db
from platform.core.schemas import IngestionResponse, IngestionStatusResponse, IngestionListResponse

router = APIRouter()

@router.post("/api/v1/datasets/{dataset_id}/ingestions", response_model=IngestionResponse)
async def upload_ingestion(dataset_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    service = IngestionService(db)
    ingestion = await service.create(dataset_id, file)
    return IngestionResponse(
        ingestion_id=str(ingestion.id),
        dataset_id=str(ingestion.dataset_id),
        status=ingestion.status,
        created_at=ingestion.created_at,
    )

@router.get("/api/v1/ingestions/{ingestion_id}", response_model=IngestionStatusResponse)
async def get_ingestion_status(ingestion_id: int, db: Session = Depends(get_db)):
    service = IngestionService(db)
    ingestion = service.get(ingestion_id)
    return IngestionStatusResponse(
        ingestion_id=str(ingestion.id),
        status=ingestion.status,
        message=None,
    )

@router.get("/api/v1/datasets/{dataset_id}/ingestions", response_model=IngestionListResponse)
async def list_dataset_ingestions(dataset_id: int, page: int = 1, page_size: int = 10, db: Session = Depends(get_db)):
    service = IngestionService(db)
    ingestions, total = service.list_for_dataset(dataset_id, page, page_size)
    return IngestionListResponse(
        ingestions=[
            IngestionResponse(
                ingestion_id=str(i.id),
                dataset_id=str(i.dataset_id),
                status=i.status,
                created_at=i.created_at,
            ) for i in ingestions
        ],
        total=total,
    )