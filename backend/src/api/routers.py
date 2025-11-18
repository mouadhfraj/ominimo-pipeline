"""
Routers Module - Enhanced with Airflow Integration
Supports both direct execution and Airflow-scheduled execution
All runs and logs saved to database for both methods
"""

import json
import sys
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from loguru import logger

from .serializers import PipelineRunRequest, AirflowTriggerRequest

sys.path.insert(0, '/app/backend')
from ..repo import (
    get_db,
    MetadataRepository,
    PipelineRunRepository,
    PipelineLogRepository
)

from ..services.pipeline import MetadataPipeline

router = APIRouter()

# Configuration
AIRFLOW_BASE_URL = "http://ominimo-airflow-webserver:8080/api/v1"
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"


active_pipelines: Dict[str, MetadataPipeline] = {}



def serialize_for_json(obj: Any) -> Any:
    """
    Recursively convert datetime objects to ISO format strings for JSON serialization
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj


def get_airflow_auth():
    """Get Airflow authentication tuple"""
    return (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)




@router.get("/")
async def root():
    return {
        "service": "Ominimo Motor Insurance Pipeline",
        "version": "2.0.0",
        "status": "running",
        "features": ["direct_execution", "airflow_scheduling"],
        "timestamp": datetime.now().isoformat()
    }


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"


    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/health",
            auth=get_airflow_auth(),
            timeout=5
        )
        airflow_status = "healthy" if response.status_code == 200 else f"unhealthy: {response.status_code}"
    except Exception as e:
        airflow_status = f"unhealthy: {str(e)}"

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_pipelines": len(active_pipelines),
        "database": db_status,
        "airflow": airflow_status
    }




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

        if "dataflows" not in metadata_json:
            raise HTTPException(400, "Invalid metadata: missing 'dataflows'")

        dataflow = metadata_json.get("dataflows", [{}])[0]
        name = file.filename.replace(".json", "")
        version = dataflow.get("version", "1.0.0")
        description = dataflow.get("description", "")

        existing = MetadataRepository.get_by_name(db, name)

        if existing:
            metadata = MetadataRepository.update(
                db, name, version=version, content=metadata_json, description=description
            )
            message = "Metadata updated successfully"
            logger.info(f"Updated metadata: {name} v{version}")
        else:
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


@router.delete("/metadata/{metadata_name}")
async def delete_metadata(metadata_name: str, hard_delete: bool = False, db: Session = Depends(get_db)):
    """Delete metadata file"""
    success = MetadataRepository.delete(db, metadata_name, soft=not hard_delete)

    if not success:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    delete_type = "deleted" if hard_delete else "deactivated"
    logger.info(f"Metadata {metadata_name} {delete_type}")
    return {"message": f"Metadata '{metadata_name}' {delete_type} successfully"}






def run_pipeline_background(pipeline_id: str, metadata_name: str, execution_method: str = "direct"):
    """Runs pipeline in background thread with database logging"""
    from ..repo.database import SessionLocal
    db = SessionLocal()

    try:
        logger.info(f"Pipeline starting [{pipeline_id}] with metadata '{metadata_name}' via {execution_method}")

        PipelineRunRepository.update_status(db, pipeline_id, "running")

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Pipeline {pipeline_id} started with metadata: {metadata_name} (method: {execution_method})",
            stage="initialization"
        )

        pipeline = MetadataPipeline(metadata_name)
        active_pipelines[pipeline_id] = pipeline

        stats = pipeline.execute()


        valid_count = 0
        invalid_count = 0

        if 'validation_ok' in pipeline.dataframes:
            valid_count = pipeline.dataframes['validation_ok'].count()
        if 'validation_ko' in pipeline.dataframes:
            invalid_count = pipeline.dataframes['validation_ko'].count()

        total_count = valid_count + invalid_count


        serialized_stages = serialize_for_json(stats.get("stages", {}))


        PipelineRunRepository.update_metrics(
            db,
            pipeline_id,
            total_records=total_count,
            valid_records=valid_count,
            invalid_records=invalid_count,
            stages=serialized_stages
        )


        PipelineRunRepository.update_status(
            db, pipeline_id, "success", end_time=datetime.utcnow()
        )


        for stage_name, stage_data in stats.get("stages", {}).items():
            PipelineLogRepository.create(
                db, pipeline_id, "INFO",
                f"Stage {stage_name} completed with status: {stage_data.get('status', 'unknown')}",
                stage=stage_name,
                details=serialize_for_json(stage_data)
            )

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Pipeline completed successfully. Valid: {valid_count}, Invalid: {invalid_count}",
            stage="completion"
        )

        logger.info(f"Pipeline finished [{pipeline_id}]")

    except Exception as e:
        logger.error(f"Pipeline error [{pipeline_id}]: {e}")

        try:
            PipelineRunRepository.update_status(
                db, pipeline_id, "failed",
                end_time=datetime.utcnow(),
                error_message=str(e)
            )

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


@router.post("/pipeline/run/direct")
async def run_pipeline_direct(
    request: PipelineRunRequest,
    background: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Execute pipeline directly (not through Airflow)"""
    metadata_name = request.metadata_path

    metadata = MetadataRepository.get_by_name(db, metadata_name)

    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found in database")

    if not metadata.is_active:
        raise HTTPException(400, f"Metadata '{metadata_name}' is not active")

    pipeline_id = f"direct_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

    PipelineRunRepository.create(
        db,
        pipeline_id=pipeline_id,
        metadata_name=metadata_name,
        status="queued"
    )

    PipelineLogRepository.create(
        db, pipeline_id, "INFO",
        f"Pipeline queued with metadata: {metadata_name} v{metadata.version} (direct execution)",
        stage="initialization",
        details={"metadata_version": metadata.version, "execution_method": "direct"}
    )

    if request.async_execution:
        background.add_task(
            run_pipeline_background,
            pipeline_id,
            metadata_name,
            "direct"
        )

        return {
            "message": "Pipeline started (direct execution)",
            "pipeline_id": pipeline_id,
            "metadata_name": metadata_name,
            "metadata_version": metadata.version,
            "execution_method": "direct",
            "status": "queued",
            "check_status_url": f"/pipeline/status/{pipeline_id}"
        }


    run_pipeline_background(pipeline_id, metadata_name, "direct")

    run = PipelineRunRepository.get_by_id(db, pipeline_id)

    return {
        "pipeline_id": run.pipeline_id,
        "metadata_name": run.metadata_name,
        "execution_method": "direct",
        "status": run.status,
        "start_time": run.start_time.isoformat(),
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "duration_seconds": run.duration_seconds,
        "total_records": run.total_records,
        "valid_records": run.valid_records,
        "invalid_records": run.invalid_records,
        "valid_percentage": run.valid_percentage,
        "stages": run.stages
    }



# Airflow Pipeline Execution


@router.post("/pipeline/run/airflow")
async def run_pipeline_airflow(
    request: AirflowTriggerRequest,
    db: Session = Depends(get_db)
):
    """Trigger pipeline execution through Airflow"""
    metadata_name = request.metadata_name

    metadata = MetadataRepository.get_by_name(db, metadata_name)

    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found in database")

    if not metadata.is_active:
        raise HTTPException(400, f"Metadata '{metadata_name}' is not active")


    pipeline_id = f"airflow_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


    PipelineRunRepository.create(
        db,
        pipeline_id=pipeline_id,
        metadata_name=metadata_name,
        status="queued_airflow"
    )

    PipelineLogRepository.create(
        db, pipeline_id, "INFO",
        f"Pipeline queued with metadata: {metadata_name} v{metadata.version} (Airflow execution)",
        stage="initialization",
        details={"metadata_version": metadata.version, "execution_method": "airflow"}
    )


    dag_id = "motor_policy_ingestion"

    try:

        conf = {
            "pipeline_id": pipeline_id,
            "metadata_name": metadata_name
        }

        response = requests.post(
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns",
            auth=get_airflow_auth(),
            json={
                "conf": conf,
                "dag_run_id": pipeline_id
            },
            headers={"Content-Type": "application/json"}
        )

        if response.status_code not in [200, 201]:
            error_msg = f"Failed to trigger Airflow DAG: {response.status_code} - {response.text}"
            logger.error(error_msg)

            PipelineRunRepository.update_status(
                db, pipeline_id, "failed",
                end_time=datetime.utcnow(),
                error_message=error_msg
            )

            raise HTTPException(500, error_msg)

        airflow_response = response.json()

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Airflow DAG triggered successfully: {dag_id}",
            stage="airflow_trigger",
            details={"dag_id": dag_id, "dag_run_id": pipeline_id}
        )

        return {
            "message": "Pipeline triggered via Airflow",
            "pipeline_id": pipeline_id,
            "metadata_name": metadata_name,
            "metadata_version": metadata.version,
            "execution_method": "airflow",
            "dag_id": dag_id,
            "dag_run_id": airflow_response.get("dag_run_id"),
            "status": "queued_airflow",
            "airflow_url": f"{AIRFLOW_BASE_URL.replace('/api/v1', '')}/dags/{dag_id}/grid",
            "check_status_url": f"/pipeline/status/{pipeline_id}"
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to connect to Airflow: {str(e)}"
        logger.error(error_msg)

        PipelineRunRepository.update_status(
            db, pipeline_id, "failed",
            end_time=datetime.utcnow(),
            error_message=error_msg
        )

        raise HTTPException(503, "Airflow service unavailable")


@router.get("/airflow/dags")
async def list_airflow_dags():
    """List available Airflow DAGs"""
    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/dags",
            auth=get_airflow_auth(),
            params={"limit": 100}
        )

        if response.status_code != 200:
            raise HTTPException(500, f"Failed to fetch DAGs: {response.status_code}")

        dags_data = response.json()

        return {
            "total_dags": dags_data.get("total_entries", 0),
            "dags": [
                {
                    "dag_id": dag["dag_id"],
                    "is_paused": dag["is_paused"],
                    "is_active": dag["is_active"],
                    "description": dag.get("description", ""),
                    "tags": dag.get("tags", [])
                }
                for dag in dags_data.get("dags", [])
            ]
        }
    except requests.exceptions.RequestException as e:
        raise HTTPException(503, f"Airflow service unavailable: {str(e)}")


@router.get("/airflow/dag/{dag_id}/runs")
async def get_airflow_dag_runs(dag_id: str, limit: int = 10):
    """Get DAG runs from Airflow"""
    try:
        response = requests.get(
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns",
            auth=get_airflow_auth(),
            params={"limit": limit, "order_by": "-execution_date"}
        )

        if response.status_code != 200:
            raise HTTPException(500, f"Failed to fetch DAG runs: {response.status_code}")

        runs_data = response.json()

        return {
            "dag_id": dag_id,
            "total_runs": runs_data.get("total_entries", 0),
            "runs": [
                {
                    "dag_run_id": run["dag_run_id"],
                    "state": run["state"],
                    "execution_date": run["execution_date"],
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "conf": run.get("conf", {})
                }
                for run in runs_data.get("dag_runs", [])
            ]
        }
    except requests.exceptions.RequestException as e:
        raise HTTPException(503, f"Airflow service unavailable: {str(e)}")




@router.post("/pipeline/run")
async def run_pipeline_legacy(
    request: PipelineRunRequest,
    background: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Legacy endpoint - defaults to direct execution"""
    return await run_pipeline_direct(request, background, db)




@router.get("/pipeline/status/{pipeline_id}")
async def pipeline_status(pipeline_id: str, db: Session = Depends(get_db)):
    """Get pipeline run status from database with stage details"""
    run = PipelineRunRepository.get_by_id(db, pipeline_id)

    if not run:
        raise HTTPException(404, f"Pipeline not found: {pipeline_id}")


    logs = PipelineLogRepository.get_by_pipeline(db, pipeline_id, limit=100)


    execution_method = "airflow" if pipeline_id.startswith("airflow_") else "direct"

    return {
        "pipeline_id": run.pipeline_id,
        "metadata_name": run.metadata_name,
        "execution_method": execution_method,
        "status": run.status,
        "start_time": run.start_time.isoformat(),
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "duration_seconds": run.duration_seconds,
        "total_records": run.total_records,
        "valid_records": run.valid_records,
        "invalid_records": run.invalid_records,
        "valid_percentage": run.valid_percentage,
        "stages": run.stages,
        "error_message": run.error_message,
        "log_count": len(logs)
    }


@router.get("/pipeline/runs")
async def list_pipeline_runs(
    limit: int = 10,
    status: str = None,
    metadata_name: str = None,
    execution_method: str = None,
    db: Session = Depends(get_db)
):
    """List pipeline runs from database with execution method filter"""
    runs = PipelineRunRepository.get_all(
        db, status=status, metadata_name=metadata_name, limit=limit
    )


    if execution_method:
        if execution_method == "airflow":
            runs = [r for r in runs if r.pipeline_id.startswith("airflow_")]
        elif execution_method == "direct":
            runs = [r for r in runs if r.pipeline_id.startswith("direct_")]

    return {
        "total": len(runs),
        "runs": [
            {
                "pipeline_id": r.pipeline_id,
                "metadata_name": r.metadata_name,
                "execution_method": "airflow" if r.pipeline_id.startswith("airflow_") else "direct",
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


@router.get("/pipeline/{pipeline_id}/stages")
async def get_pipeline_stages(pipeline_id: str, db: Session = Depends(get_db)):
    """Get detailed stage information for a pipeline run"""
    run = PipelineRunRepository.get_by_id(db, pipeline_id)

    if not run:
        raise HTTPException(404, f"Pipeline not found: {pipeline_id}")

    if not run.stages:
        return {
            "pipeline_id": pipeline_id,
            "message": "No stage information available yet",
            "stages": []
        }


    logs = PipelineLogRepository.get_by_pipeline(db, pipeline_id, limit=1000)

    stage_logs = {}
    for log in logs:
        if log.stage:
            if log.stage not in stage_logs:
                stage_logs[log.stage] = []
            stage_logs[log.stage].append({
                "timestamp": log.timestamp.isoformat(),
                "level": log.level,
                "message": log.message
            })


    stages_detail = []
    for stage_name, stage_data in run.stages.items():
        stages_detail.append({
            "name": stage_name,
            "status": stage_data.get("status", "unknown"),
            "start_time": stage_data.get("start_time"),
            "end_time": stage_data.get("end_time"),
            "data": stage_data,
            "logs": stage_logs.get(stage_name, [])
        })

    return {
        "pipeline_id": pipeline_id,
        "total_stages": len(stages_detail),
        "stages": stages_detail
    }


@router.delete("/pipeline/{pipeline_id}")
async def cancel_pipeline(pipeline_id: str, db: Session = Depends(get_db)):
    """Cancel running pipeline"""
    if pipeline_id not in active_pipelines:
        raise HTTPException(404, f"Pipeline is not running: {pipeline_id}")

    try:
        active_pipelines[pipeline_id].stop()
        del active_pipelines[pipeline_id]

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





@router.get("/logs/{pipeline_id}")
async def get_logs(
    pipeline_id: str,
    level: str = None,
    stage: str = None,
    limit: int = 1000,
    db: Session = Depends(get_db)
):
    """Get pipeline logs from database with optional stage filter"""
    logs = PipelineLogRepository.get_by_pipeline(db, pipeline_id, level=level, limit=limit)


    if stage:
        logs = [log for log in logs if log.stage == stage]

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



@router.get("/stats")
async def statistics(db: Session = Depends(get_db)):
    """Get pipeline statistics from database"""
    stats = PipelineRunRepository.get_statistics(db)


    all_runs = PipelineRunRepository.get_all(db, limit=10000)
    direct_runs = [r for r in all_runs if r.pipeline_id.startswith("direct_")]
    airflow_runs = [r for r in all_runs if r.pipeline_id.startswith("airflow_")]

    stats["execution_methods"] = {
        "direct": {
            "total": len(direct_runs),
            "successful": sum(1 for r in direct_runs if r.status == "success"),
            "failed": sum(1 for r in direct_runs if r.status == "failed")
        },
        "airflow": {
            "total": len(airflow_runs),
            "successful": sum(1 for r in airflow_runs if r.status == "success"),
            "failed": sum(1 for r in airflow_runs if r.status == "failed")
        }
    }

    metadata_count = len(MetadataRepository.get_all(db, active_only=True))
    stats["active_metadata_files"] = metadata_count

    return stats


@router.get("/stats/metadata/{metadata_name}")
async def metadata_statistics(metadata_name: str, db: Session = Depends(get_db)):
    """Get statistics for specific metadata"""
    metadata = MetadataRepository.get_by_name(db, metadata_name)
    if not metadata:
        raise HTTPException(404, f"Metadata '{metadata_name}' not found")

    runs = PipelineRunRepository.get_all(db, metadata_name=metadata_name, limit=1000)

    total = len(runs)
    success = sum(1 for r in runs if r.status == "success")
    failed = sum(1 for r in runs if r.status == "failed")


    direct_runs = [r for r in runs if r.pipeline_id.startswith("direct_")]
    airflow_runs = [r for r in runs if r.pipeline_id.startswith("airflow_")]

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
        "execution_methods": {
            "direct": len(direct_runs),
            "airflow": len(airflow_runs)
        },
        "total_valid_records": valid_records_sum,
        "total_invalid_records": invalid_records_sum,
        "average_duration_seconds": (duration_sum / total) if total > 0 else 0,
        "last_run": runs[0].start_time.isoformat() if runs else None
    }