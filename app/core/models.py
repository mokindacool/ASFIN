from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.sql import func

from app.core.database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False, index=True)
    process_type = Column(String, nullable=False)
    description = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_deleted = Column(Boolean, default=False, nullable=False)


class DatasetUpload(Base):
    __tablename__ = "dataset_uploads"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False, index=True)
    original_filename = Column(String, nullable=False)
    stored_path = Column(String, nullable=False)
    content_type = Column(String, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())
