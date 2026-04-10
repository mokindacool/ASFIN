from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class IngestionRequest(BaseModel):
    file: bytes 
    description: Optional[str] = None

class IngestionResponse(BaseModel):
    ingestion_id: str
    dataset_id: str
    status: str
    created_at: datetime

class IngestionStatusResponse(BaseModel):
    ingestion_id: str
    status: str
    message: Optional[str] = None

class IngestionListResponse(BaseModel):
    ingestions: List[IngestionResponse]
    total: int