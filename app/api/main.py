from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.core.database import check_connection

app = FastAPI(title="ASFINT Data Platform")


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
