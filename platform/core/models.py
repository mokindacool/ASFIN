from __future__ import annotations
import datetime as dt
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

# Base defined in platform/core/database.py
from .database import Base


class Dataset(Base):
    """
    datasets: registry of known dataset types (configuration card).
    """
    __tablename__ = "datasets"

    id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    process_type: Mapped[str] = mapped_column(Text, nullable=False)  # 'FR'|'ABSA'|'OASIS'|'CONTINGENCY'|'RECONCILE'
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # JSONB blobs; validated at the API/schema layer later
    schema_def: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB, nullable=True)
    validation_cfg: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),  # app-side update
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("TRUE"),
    )

    # Relationships
    ingestions: Mapped[List["Ingestion"]] = relationship(
        back_populates="dataset",
        cascade="save-update, merge",
        passive_deletes=True,
    )
    published_versions: Mapped[List["PublishedVersion"]] = relationship(
        back_populates="dataset",
        cascade="save-update, merge",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("idx_datasets_process_type", "process_type"),
    )


class Ingestion(Base):
    """
    ingestions: one row per file upload event (= one pipeline run).
    """
    __tablename__ = "ingestions"

    id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    dataset_id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("datasets.id"),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
    )
    original_filename: Mapped[str] = mapped_column(Text, nullable=False)
    file_ext: Mapped[str] = mapped_column(Text, nullable=False)

    raw_path: Mapped[str] = mapped_column(Text, nullable=False)
    clean_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    file_size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    file_sha256: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    row_count_raw: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    row_count_clean: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    triggered_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[Optional[dt.datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    dataset: Mapped["Dataset"] = relationship(back_populates="ingestions")

    validation_results: Mapped[List["ValidationResult"]] = relationship(
        back_populates="ingestion",
        cascade="all, delete-orphan",
        passive_deletes=True,  # matches ON DELETE CASCADE in FK
    )

    published_versions: Mapped[List["PublishedVersion"]] = relationship(
        back_populates="ingestion",
        cascade="save-update, merge",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("idx_ingestions_dataset_id", "dataset_id"),
        Index("idx_ingestions_status", "status"),
        Index("idx_ingestions_created_at", created_at.desc()),
        # Idempotency partial unique index (Postgres-only)
        # same file + same dataset cannot reprocess unless prior attempt failed/validation_failed
        Index(
            "idx_ingestions_idempotent",
            "dataset_id",
            "file_sha256",
            unique=True,
            postgresql_where=text("status NOT IN ('failed', 'validation_failed')"),
        ),
    )


class ValidationResult(Base):
    """
    validation_results: one row per check per ingestion (immutable after creation).
    """
    __tablename__ = "validation_results"

    id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    ingestion_id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestions.id", ondelete="CASCADE"),
        nullable=False,
    )

    check_name: Mapped[str] = mapped_column(Text, nullable=False)  # 'schema'|'shape'|'join'|'drift'
    status: Mapped[str] = mapped_column(Text, nullable=False)      # 'pass'|'fail'|'warn'|'skipped'
    severity: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'error'"),
    )  # 'error' blocks publish

    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    ran_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    ingestion: Mapped["Ingestion"] = relationship(back_populates="validation_results")

    __table_args__ = (
        Index("idx_val_results_ingestion_id", "ingestion_id"),
    )


class PublishedVersion(Base):
    """
    published_versions: immutable record of every publish event (version ledger).
    """
    __tablename__ = "published_versions"

    id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    dataset_id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("datasets.id"),
        nullable=False,
    )
    ingestion_id: Mapped[Any] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestions.id"),
        nullable=False,
    )

    version_number: Mapped[int] = mapped_column(Integer, nullable=False)  # monotonic per dataset
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)

    file_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    schema_snapshot: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    published_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    published_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    is_latest: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("TRUE"),
    )

    dataset: Mapped["Dataset"] = relationship(back_populates="published_versions")
    ingestion: Mapped["Ingestion"] = relationship(back_populates="published_versions")

    __table_args__ = (
        Index("idx_pub_versions_dataset_version", "dataset_id", "version_number", unique=True),
        Index(
            "idx_pub_versions_latest",
            "dataset_id",
            "is_latest",
            postgresql_where=text("is_latest = TRUE"),
        ),
    )