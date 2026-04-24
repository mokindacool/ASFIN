from typing import List

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.core import job_runner
from app.core.database import SessionLocal, get_db
from app.core.models import Ingestion, ValidationResult as ValidationResultModel
from app.core.schemas import IngestionList, IngestionOut, ValidationResultOut
from app.services.ingestion_service import IngestionService
from app.services.validation_service import ValidationService

router = APIRouter(tags=["ingestions"])

# Module-level alias so tests can monkeypatch it without touching the job_runner module.
job_submit = job_runner.submit


def _validate_ingestion(ingestion_id: int) -> None:
    db = SessionLocal()
    try:
        ValidationService(db).run(ingestion_id)
    except Exception:
        pass  # ValidationService already persists status + error_message on failure
    finally:
        db.close()


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
    ingestion = await IngestionService(db).create(dataset_id, file)
    job_submit(_validate_ingestion, ingestion.id)
    return ingestion


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


@router.get(
    "/api/v1/ingestions/{ingestion_id}/validation",
    response_model=List[ValidationResultOut],
)
def get_validation_results(ingestion_id: int, db: Session = Depends(get_db)):
    """Return all per-check validation results for an ingestion."""
    ingestion = db.query(Ingestion).filter(Ingestion.id == ingestion_id).first()
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")
    return (
        db.query(ValidationResultModel)
        .filter(ValidationResultModel.ingestion_id == ingestion_id)
        .all()
    )


@router.get(
    "/api/v1/ingestions/{ingestion_id}/validation/{check}",
    response_model=ValidationResultOut,
)
def get_validation_check(ingestion_id: int, check: str, db: Session = Depends(get_db)):
    """Return the result for a single named check (e.g. 'schema', 'shape')."""
    result = (
        db.query(ValidationResultModel)
        .filter(
            ValidationResultModel.ingestion_id == ingestion_id,
            ValidationResultModel.check_name == check,
        )
        .first()
    )
    if not result:
        raise HTTPException(status_code=404, detail=f"No result for check '{check}'")
    return result
