from pydantic import BaseModel
from typing import Dict, Optional
from datetime import datetime


class PipelineRunRequest(BaseModel):
    metadata_path: str
    async_execution: bool = True


class PipelineStatus(BaseModel):
    pipeline_id: str
    status: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    stages: Dict


class MetadataInfo(BaseModel):
    name: str
    path: str
    version: str
    description: str
