from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime


class PipelineRunRequest(BaseModel):
    """Request model for direct pipeline execution"""
    metadata_path: str = Field(
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
                "metadata_path": "motor_policy",
                "async_execution": True
            }
        }


class AirflowTriggerRequest(BaseModel):
    """Request model for Airflow pipeline execution"""
    metadata_name: str = Field(
        ...,
        description="Name of metadata in database to execute via Airflow",
        example="motor_policy"
    )
    conf: Optional[Dict] = Field(
        default=None,
        description="Additional configuration for Airflow DAG run"
    )

    class Config:
        schema_extra = {
            "example": {
                "metadata_name": "motor_policy",
                "conf": {
                    "custom_param": "value"
                }
            }
        }


class PipelineStatus(BaseModel):
    """Pipeline execution status response"""
    pipeline_id: str
    metadata_name: str
    execution_method: str
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
    log_count: Optional[int]


class PipelineStageDetail(BaseModel):
    """Detailed information about a pipeline stage"""
    name: str
    status: str
    start_time: Optional[str]
    end_time: Optional[str]
    data: Dict
    logs: List[Dict]


class MetadataInfo(BaseModel):
    """Metadata file information"""
    name: str
    version: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    is_active: bool


class MetadataUploadResponse(BaseModel):
    """Response for metadata upload"""
    message: str
    name: str
    version: str
    created_at: datetime
    updated_at: datetime


class PipelineRunResponse(BaseModel):
    """Response for pipeline execution request"""
    message: str
    pipeline_id: str
    metadata_name: str
    metadata_version: str
    execution_method: str
    status: str
    check_status_url: str
    dag_id: Optional[str] = None
    dag_run_id: Optional[str] = None
    airflow_url: Optional[str] = None


class AirflowDAGInfo(BaseModel):
    """Airflow DAG information"""
    dag_id: str
    is_paused: bool
    is_active: bool
    description: Optional[str]
    tags: List[str]


class AirflowDAGRunInfo(BaseModel):
    """Airflow DAG run information"""
    dag_run_id: str
    state: str
    execution_date: str
    start_date: Optional[str]
    end_date: Optional[str]
    conf: Optional[Dict]


class PipelineStatistics(BaseModel):
    """Pipeline execution statistics"""
    total_runs: int
    successful_runs: int
    failed_runs: int
    running_pipelines: int
    success_rate: float
    execution_methods: Dict[str, Dict[str, int]]
    active_metadata_files: int


class LogEntry(BaseModel):
    """Pipeline log entry"""
    timestamp: str
    level: str
    stage: Optional[str]
    message: str
    details: Optional[Dict]