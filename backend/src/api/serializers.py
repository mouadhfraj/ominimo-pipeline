from pydantic import BaseModel, Field
from typing import Dict, Optional
from datetime import datetime


class PipelineRunRequest(BaseModel):
    """Request model for pipeline execution - now uses metadata name instead of path"""
    metadata_path: str = Field(
        ...,
        description="Name of metadata in database (or legacy path)",
        example="motor_policy"
    )
    async_execution: bool = Field(
        default=True,
        description="Run pipeline asynchronously in background"
    )

    class Config:
        schema_extra = {
            "example": {
                "metadata_path": "motor_policy",
                "async_execution": True
            }
        }


class PipelineRunRequestV2(BaseModel):
    """V2 API - explicitly uses metadata_name"""
    metadata_name: str = Field(
        ...,
        description="Name of metadata in database",
        example="motor_policy"
    )
    async_execution: bool = Field(
        default=True,
        description="Run pipeline asynchronously in background"
    )

    class Config:
        schema_extra = {
            "example": {
                "metadata_name": "motor_policy",
                "async_execution": True
            }
        }


class PipelineStatus(BaseModel):
    pipeline_id: str
    metadata_name: str
    status: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    total_records: Optional[int]
    valid_records: Optional[int]
    invalid_records: Optional[int]
    valid_percentage: Optional[float]
    stages: Optional[Dict]
    error_message: Optional[str]


class MetadataInfo(BaseModel):
    name: str
    version: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    is_active: bool


class MetadataUploadResponse(BaseModel):
    message: str
    name: str
    version: str
    created_at: datetime
    updated_at: datetime


class PipelineRunResponse(BaseModel):
    message: str
    pipeline_id: str
    metadata_name: str
    metadata_version: str
    status: str
    check_status_url: str