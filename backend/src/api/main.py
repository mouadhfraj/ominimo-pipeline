from fastapi import FastAPI
from loguru import logger
from pathlib import Path
from datetime import datetime

from routers import router

app = FastAPI(
    title="Ominimo Motor Insurance Pipeline API",
    description="API for managing and monitoring motor insurance data pipelines",
    version="1.0.0"
)

app.include_router(router)


@app.on_event("startup")
async def startup():
    logger.info("Starting API service...")
    Path("/backend/metadata").mkdir(parents=True, exist_ok=True)
    Path("/backend/logs").mkdir(parents=True, exist_ok=True)
    logger.info("API is ready")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
