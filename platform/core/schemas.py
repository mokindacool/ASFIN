from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class MessageResponse(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    detail: str


class HealthResponse(BaseModel):
    status: str = "ok"


class PaginationParams(BaseModel):
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class UUIDModel(BaseModel):
    id: UUID

    model_config = ConfigDict(from_attributes=True)


class ColumnDef(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    dtype: str = Field(..., min_length=1, max_length=100)
    nullable: bool | None = None
    description: str | None = None


class DatasetBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    process_type: str = Field(..., min_length=1, max_length=64)
    owner: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    schema_def: list[ColumnDef] | None = None
    validation_cfg: dict[str, Any] | None = None


class DatasetCreate(DatasetBase):
    pass


class DatasetUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    process_type: str | None = Field(default=None, min_length=1, max_length=64)
    owner: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    schema_def: list[ColumnDef] | None = None
    validation_cfg: dict[str, Any] | None = None
    is_active: bool | None = None


class DatasetResponse(UUIDModel, DatasetBase):
    created_at: datetime
    updated_at: datetime
    is_active: bool


class DatasetListResponse(BaseModel):
    items: list[DatasetResponse]
    total: int


class JSONPayload(BaseModel):
    data: dict[str, Any]
