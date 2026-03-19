from __future__ import annotations
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session
from .config import settings


class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass

engine = create_engine(
    settings.DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)

def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency: yields a DB session and ensures it closes
    Use in dependencies.py as: Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()