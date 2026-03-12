from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.api.routers import datasets
from app.core.database import check_connection, engine
from app.core.models import Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup.
    # Alembic migrations (added by Jeffrey in a later sprint) will replace this.
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="ASFINT Data Platform", lifespan=lifespan)

app.include_router(datasets.router)


@app.get("/healthz")
def healthz():
    """
    Liveness check.
    Returns 200 if the API process is running.
    Does not check any dependencies.
    """
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    """
    Readiness check.
    Returns 200 if the API can reach the database.
    Returns 503 if the database is unreachable.
    """
    if check_connection():
        return {"status": "ok", "database": "reachable"}
    return JSONResponse(
        status_code=503,
        content={"status": "unavailable", "database": "unreachable"},
    )
