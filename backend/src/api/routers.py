"""
Routers Module - Database-First Implementation
All metadata operations now use PostgreSQL database
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from loguru import logger

from .serializers import PipelineRunRequest

sys.path.insert(0, '/app/backend')
from ..repo import (
    get_db,
    MetadataRepository,
    PipelineRunRepository,
    PipelineLogRepository
)

from ..services.pipeline import MetadataPipeline

router = APIRouter()

# In-memory storage for active pipelines only
active_pipelines: Dict[str, MetadataPipeline] = {}


# --------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------

def serialize_for_json(obj: Any) -> Any:
    """
    Recursively convert datetime objects to ISO format strings for JSON serialization

    Args:
        obj: Object to serialize (can be dict, list, datetime, or primitive)

    Returns:
        JSON-serializable version of the object
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj


# --------------------------------------------------------------------
# Root & Health
# --------------------------------------------------------------------

@router.get("/")
async def root():
    return {
        "service": "Ominimo Motor Insurance Pipeline",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    try:
        # Test database connection
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_pipelines": len(active_pipelines),
        "database": db_status
    }


# --------------------------------------------------------------------
# Metadata Endpoints - DATABASE FIRST
# --------------------------------------------------------------------

@router.get("/metadata")
async def list_metadata(active_only: bool = True, db: Session = Depends(get_db)):
    """List all metadata files from database"""
    try:
        metadata_files = MetadataRepository.get_all(db, active_only=active_only)

        return {
            "metadata_files": [
                {
                    "name": m.name,
                    "version": m.version,
                    "description": m.description,
                    "created_at": m.created_at.isoformat(),
                    "updated_at": m.updated_at.isoformat(),
                    "is_active": m.is_active
                }
                for m in metadata_files
            ]
        }
    except Exception as e:
        logger.error(f"Error listing metadata: {e}")
        raise HTTPException(500, f"Error listing metadata: {e}")


@router.get("/metadata/{metadata_name}")
async def get_metadata(metadata_name: str, db: Session = Depends(get_db)):
    """Get specific metadata file from database"""
    metadata = MetadataRepository.get_by_name(db, metadata_name)

    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    return {
        "name": metadata.name,
        "version": metadata.version,
        "description": metadata.description,
        "content": metadata.content,
        "created_at": metadata.created_at.isoformat(),
        "updated_at": metadata.updated_at.isoformat(),
        "is_active": metadata.is_active
    }


@router.post("/metadata/upload")
async def upload_metadata(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Upload new metadata file to database"""
    if not file.filename.endswith(".json"):
        raise HTTPException(400, "Only JSON files allowed")

    try:
        content = await file.read()
        metadata_json = json.loads(content)

        # Validate structure
        if "dataflows" not in metadata_json:
            raise HTTPException(400, "Invalid metadata: missing 'dataflows'")

        dataflow = metadata_json.get("dataflows", [{}])[0]
        name = file.filename.replace(".json", "")
        version = dataflow.get("version", "1.0.0")
        description = dataflow.get("description", "")

        # Check if metadata already exists
        existing = MetadataRepository.get_by_name(db, name)

        if existing:
            # Update existing
            metadata = MetadataRepository.update(
                db, name, version=version, content=metadata_json, description=description
            )
            message = "Metadata updated successfully"
            logger.info(f"Updated metadata: {name} v{version}")
        else:
            # Create new
            metadata = MetadataRepository.create(
                db, name, version, metadata_json, description
            )
            message = "Metadata uploaded successfully"
            logger.info(f"Created metadata: {name} v{version}")

        return {
            "message": message,
            "name": metadata.name,
            "version": metadata.version,
            "created_at": metadata.created_at.isoformat(),
            "updated_at": metadata.updated_at.isoformat()
        }

    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON format")
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(500, f"Upload error: {e}")


@router.put("/metadata/{metadata_name}")
async def update_metadata(
    metadata_name: str,
    version: str = None,
    description: str = None,
    content: Dict = None,
    db: Session = Depends(get_db)
):
    """Update existing metadata"""
    metadata = MetadataRepository.update(db, metadata_name, version, content, description)

    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    return {
        "message": "Metadata updated successfully",
        "name": metadata.name,
        "version": metadata.version,
        "updated_at": metadata.updated_at.isoformat()
    }


@router.delete("/metadata/{metadata_name}")
async def delete_metadata(metadata_name: str, hard_delete: bool = False, db: Session = Depends(get_db)):
    """Delete metadata file (soft delete by default)"""
    success = MetadataRepository.delete(db, metadata_name, soft=not hard_delete)

    if not success:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    delete_type = "deleted" if hard_delete else "deactivated"
    logger.info(f"Metadata {metadata_name} {delete_type}")
    return {"message": f"Metadata '{metadata_name}' {delete_type} successfully"}


# --------------------------------------------------------------------
# Pipeline Execution - DATABASE INTEGRATED
# --------------------------------------------------------------------

def run_pipeline_background(pipeline_id: str, metadata_name: str, db_session_maker):
    """Runs pipeline in background thread with database logging"""

    # Create new database session for background task
    from ..repo.database import SessionLocal
    db = SessionLocal()

    try:
        logger.info(f"Pipeline starting [{pipeline_id}] with metadata '{metadata_name}'")

        # Update status to running
        PipelineRunRepository.update_status(db, pipeline_id, "running")

        # Create log entry
        PipelineLogRepository.create(
            db, pipeline_id, "INFO", f"Pipeline {pipeline_id} started with metadata: {metadata_name}", stage="initialization"
        )

        # Execute pipeline - now using metadata NAME, not path
        pipeline = MetadataPipeline(metadata_name)
        active_pipelines[pipeline_id] = pipeline

        stats = pipeline.execute()

        # Extract metrics
        valid_count = 0
        invalid_count = 0

        if 'validation_ok' in pipeline.dataframes:
            valid_count = pipeline.dataframes['validation_ok'].count()
        if 'validation_ko' in pipeline.dataframes:
            invalid_count = pipeline.dataframes['validation_ko'].count()

        total_count = valid_count + invalid_count

        # CRITICAL FIX: Serialize stages dict to convert datetime objects to ISO strings
        serialized_stages = serialize_for_json(stats.get("stages", {}))

        # Update run with metrics
        PipelineRunRepository.update_metrics(
            db,
            pipeline_id,
            total_records=total_count,
            valid_records=valid_count,
            invalid_records=invalid_count,
            stages=serialized_stages  # Use serialized version
        )

        # Update status to success
        PipelineRunRepository.update_status(
            db, pipeline_id, "success", end_time=datetime.utcnow()
        )

        # Log success
        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Pipeline completed successfully. Valid: {valid_count}, Invalid: {invalid_count}",
            stage="completion"
        )

        logger.info(f"Pipeline finished [{pipeline_id}]")

    except Exception as e:
        logger.error(f"Pipeline error [{pipeline_id}]: {e}")

        # Update status to failed
        try:
            PipelineRunRepository.update_status(
                db, pipeline_id, "failed",
                end_time=datetime.utcnow(),
                error_message=str(e)
            )

            # Log error
            PipelineLogRepository.create(
                db, pipeline_id, "ERROR",
                f"Pipeline failed: {str(e)}",
                stage="execution",
                details={"error": str(e), "error_type": type(e).__name__}
            )
        except Exception as db_error:
            logger.error(f"Failed to update database after pipeline error: {db_error}")

    finally:
        if pipeline_id in active_pipelines:
            active_pipelines[pipeline_id].stop()
            del active_pipelines[pipeline_id]

        db.close()


@router.post("/pipeline/run")
async def run_pipeline(
    request: PipelineRunRequest,
    background: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Execute pipeline with database tracking - using metadata name"""

    # Extract metadata name from request
    metadata_name = request.metadata_path

    # Check if metadata exists in database
    metadata = MetadataRepository.get_by_name(db, metadata_name)

    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found in database")

    if not metadata.is_active:
        raise HTTPException(400, f"Metadata '{metadata_name}' is not active")

    # Generate pipeline ID
    pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Create pipeline run record
    PipelineRunRepository.create(
        db,
        pipeline_id=pipeline_id,
        metadata_name=metadata_name,
        status="queued"
    )

    # Log pipeline creation
    PipelineLogRepository.create(
        db, pipeline_id, "INFO",
        f"Pipeline queued with metadata: {metadata_name} v{metadata.version}",
        stage="initialization",
        details={"metadata_version": metadata.version}
    )

    if request.async_execution:
        # Run in background
        from ..repo.database import SessionLocal
        background.add_task(
            run_pipeline_background,
            pipeline_id,
            metadata_name,
            SessionLocal
        )

        return {
            "message": "Pipeline started",
            "pipeline_id": pipeline_id,
            "metadata_name": metadata_name,
            "metadata_version": metadata.version,
            "status": "queued",
            "check_status_url": f"/pipeline/status/{pipeline_id}"
        }

    # Synchronous execution
    run_pipeline_background(pipeline_id, metadata_name, None)

    # Get updated run info
    run = PipelineRunRepository.get_by_id(db, pipeline_id)

    return {
        "pipeline_id": run.pipeline_id,
        "metadata_name": run.metadata_name,
        "status": run.status,
        "start_time": run.start_time.isoformat(),
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "duration_seconds": run.duration_seconds,
        "total_records": run.total_records,
        "valid_records": run.valid_records,
        "invalid_records": run.invalid_records,
        "valid_percentage": run.valid_percentage
    }


@router.get("/pipeline/status/{pipeline_id}")
async def pipeline_status(pipeline_id: str, db: Session = Depends(get_db)):
    """Get pipeline run status from database"""
    run = PipelineRunRepository.get_by_id(db, pipeline_id)

    if not run:
        raise HTTPException(404, f"Pipeline not found: {pipeline_id}")

    return {
        "pipeline_id": run.pipeline_id,
        "metadata_name": run.metadata_name,
        "status": run.status,
        "start_time": run.start_time.isoformat(),
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "duration_seconds": run.duration_seconds,
        "total_records": run.total_records,
        "valid_records": run.valid_records,
        "invalid_records": run.invalid_records,
        "valid_percentage": run.valid_percentage,
        "stages": run.stages,
        "error_message": run.error_message
    }


@router.get("/pipeline/runs")
async def list_pipeline_runs(
    limit: int = 10,
    status: str = None,
    metadata_name: str = None,
    db: Session = Depends(get_db)
):
    """List pipeline runs from database"""
    runs = PipelineRunRepository.get_all(
        db, status=status, metadata_name=metadata_name, limit=limit
    )

    return {
        "total": len(runs),
        "runs": [
            {
                "pipeline_id": r.pipeline_id,
                "metadata_name": r.metadata_name,
                "status": r.status,
                "start_time": r.start_time.isoformat(),
                "end_time": r.end_time.isoformat() if r.end_time else None,
                "duration_seconds": r.duration_seconds,
                "total_records": r.total_records,
                "valid_records": r.valid_records,
                "invalid_records": r.invalid_records,
                "valid_percentage": r.valid_percentage
            }
            for r in runs
        ]
    }


@router.delete("/pipeline/{pipeline_id}")
async def cancel_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    """Cancel running pipeline"""
    if pipeline_id not in active_pipelines:
        raise HTTPException(404, f"Pipeline is not running: {pipeline_id}")

    try:
        active_pipelines[pipeline_id].stop()
        del active_pipelines[pipeline_id]

        # Update database
        PipelineRunRepository.update_status(
            db, pipeline_id, "cancelled", end_time=datetime.utcnow()
        )

        PipelineLogRepository.create(
            db, pipeline_id, "WARNING",
            "Pipeline cancelled by user",
            stage="cancellation"
        )

        logger.info(f"Pipeline {pipeline_id} cancelled")
        return {"message": f"Pipeline {pipeline_id} cancelled"}

    except Exception as e:
        raise HTTPException(500, f"Error cancelling pipeline: {e}")


# --------------------------------------------------------------------
# Logs
# --------------------------------------------------------------------

@router.get("/logs/{pipeline_id}")
async def get_logs(
    pipeline_id: str,
    level: str = None,
    limit: int = 1000,
    db: Session = Depends(get_db)
):
    """Get pipeline logs from database"""
    logs = PipelineLogRepository.get_by_pipeline(db, pipeline_id, level=level, limit=limit)

    if not logs:
        raise HTTPException(404, f"No logs found for pipeline {pipeline_id}")

    return {
        "pipeline_id": pipeline_id,
        "count": len(logs),
        "logs": [
            {
                "timestamp": log.timestamp.isoformat(),
                "level": log.level,
                "stage": log.stage,
                "message": log.message,
                "details": log.details
            }
            for log in logs
        ]
    }


@router.get("/logs/recent/errors")
async def get_recent_errors(limit: int = 100, db: Session = Depends(get_db)):
    """Get recent error logs"""
    logs = PipelineLogRepository.get_recent_errors(db, limit=limit)

    return {
        "count": len(logs),
        "errors": [
            {
                "pipeline_id": log.pipeline_id,
                "timestamp": log.timestamp.isoformat(),
                "level": log.level,
                "stage": log.stage,
                "message": log.message,
                "details": log.details
            }
            for log in logs
        ]
    }


# --------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------

@router.get("/stats")
async def statistics(db: Session = Depends(get_db)):
    """Get pipeline statistics from database"""
    stats = PipelineRunRepository.get_statistics(db)

    # Add metadata count
    metadata_count = len(MetadataRepository.get_all(db, active_only=True))
    stats["active_metadata_files"] = metadata_count

    return stats


@router.get("/stats/metadata/{metadata_name}")
async def metadata_statistics(metadata_name: str, db: Session = Depends(get_db)):
    """Get statistics for specific metadata"""

    # Check if metadata exists
    metadata = MetadataRepository.get_by_name(db, metadata_name)
    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    # Get all runs for this metadata
    runs = PipelineRunRepository.get_all(db, metadata_name=metadata_name, limit=1000)

    total = len(runs)
    success = sum(1 for r in runs if r.status == "success")
    failed = sum(1 for r in runs if r.status == "failed")

    # Calculate average metrics
    valid_records_sum = sum(r.valid_records or 0 for r in runs if r.valid_records)
    invalid_records_sum = sum(r.invalid_records or 0 for r in runs if r.invalid_records)
    duration_sum = sum(r.duration_seconds or 0 for r in runs if r.duration_seconds)

    return {
        "metadata_name": metadata_name,
        "metadata_version": metadata.version,
        "total_runs": total,
        "successful_runs": success,
        "failed_runs": failed,
        "success_rate": (success / total * 100) if total > 0 else 0,
        "total_valid_records": valid_records_sum,
        "total_invalid_records": invalid_records_sum,
        "average_duration_seconds": (duration_sum / total) if total > 0 else 0,
        "last_run": runs[0].start_time.isoformat() if runs else None
    }