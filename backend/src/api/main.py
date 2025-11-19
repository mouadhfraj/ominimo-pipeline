from fastapi import FastAPI
from loguru import logger
from pathlib import Path
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from .routers import router

# Import database initialization
import sys

sys.path.insert(0, '/app/backend')


app = FastAPI(
    title="Ominimo Motor Insurance Pipeline API",
    description="API for managing and monitoring motor insurance data pipelines",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8081"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
async def startup():
    logger.info("Starting API service...")

    # Create directories
    Path("/app/metadata").mkdir(parents=True, exist_ok=True)
    Path("/app/logs").mkdir(parents=True, exist_ok=True)



    logger.info("API is ready")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down API service...")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )