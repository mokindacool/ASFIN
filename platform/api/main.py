from fastapi import FastAPI
from api.routers.datasets import router as datasets_router

app = FastAPI() # creates the app

app.include_router(datasets_router, prefix="/api/routers", tags=["datasets"])