"""
Routers Module
Contains all FastAPI routes for pipeline management
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks
from loguru import logger

from .serializers import PipelineRunRequest
from ..services.pipeline import MetadataPipeline

router = APIRouter()

# In-memory storage
pipeline_runs: Dict[str, Dict] = {}
active_pipelines: Dict[str, MetadataPipeline] = {}


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
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_pipelines": len(active_pipelines)
    }


# --------------------------------------------------------------------
# Metadata Endpoints
# --------------------------------------------------------------------

@router.get("/metadata")
async def list_metadata():
    metadata_dir = Path(r"/app/metadata")
    if not metadata_dir.exists():
        return {"metadata_files": []}

    metadata_files = []

    for file_path in metadata_dir.glob("*.json"):
        try:
            with open(file_path, "r") as f:
                metadata = json.load(f)
                dataflow = metadata.get("dataflows", [{}])[0]

                metadata_files.append({
                    "name": file_path.stem,
                    "path": str(file_path),
                    "version": dataflow.get("version", "unknown"),
                    "description": dataflow.get("description", "")
                })
        except Exception as e:
            logger.error(f"Could not read metadata {file_path}: {e}")

    return {"metadata_files": metadata_files}


@router.get("/metadata/{metadata_name}")
async def get_metadata(metadata_name: str):
    metadata_path = Path(f"/app/metadata/{metadata_name}.json")

    if not metadata_path.exists():
        raise HTTPException(404, f"Metadata file '{metadata_name}' not found")

    try:
        with open(metadata_path, "r") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(500, f"Error reading metadata: {e}")


@router.post("/metadata/upload")
async def upload_metadata(file: UploadFile = File(...)):
    if not file.filename.endswith(".json"):
        raise HTTPException(400, "Only JSON files allowed")

    try:
        content = await file.read()
        metadata = json.loads(content)

        # validate structure
        if "dataflows" not in metadata:
            raise HTTPException(400, "Invalid metadata: missing 'dataflows'")

        save_path = Path(f"/app/metadata/{file.filename}")
        with open(save_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return {
            "message": "Metadata uploaded successfully",
            "filename": file.filename,
            "path": str(save_path)
        }

    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON format")
    except Exception as e:
        raise HTTPException(500, f"Upload error: {e}")


# --------------------------------------------------------------------
# Pipeline Execution
# --------------------------------------------------------------------

def run_pipeline_background(pipeline_id: str, metadata_path: str):
    """Runs pipeline in background thread."""
    try:
        logger.info(f"Pipeline starting [{pipeline_id}]")

        pipeline = MetadataPipeline(metadata_path)
        active_pipelines[pipeline_id] = pipeline

        stats = pipeline.execute()

        pipeline_runs[pipeline_id] = stats
        pipeline_runs[pipeline_id]["pipeline_id"] = pipeline_id

        logger.info(f"Pipeline finished [{pipeline_id}]")

    except Exception as e:
        logger.error(f"Pipeline error [{pipeline_id}]: {e}")

        pipeline_runs[pipeline_id] = {
            "pipeline_id": pipeline_id,
            "status": "failed",
            "error": str(e),
            "end_time": datetime.now()
        }

    finally:
        if pipeline_id in active_pipelines:
            active_pipelines[pipeline_id].stop()
            del active_pipelines[pipeline_id]


@router.post("/pipeline/run")
async def run_pipeline(request: PipelineRunRequest, background: BackgroundTasks):
    metadata_path = Path(request.metadata_path)

    if not metadata_path.exists():
        raise HTTPException(404, f"Metadata not found: {request.metadata_path}")

    pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    pipeline_runs[pipeline_id] = {
        "pipeline_id": pipeline_id,
        "status": "queued",
        "metadata_path": request.metadata_path,
        "start_time": datetime.now(),
        "async_execution": request.async_execution
    }

    if request.async_execution:
        background.add_task(run_pipeline_background, pipeline_id, request.metadata_path)

        return {
            "message": "Pipeline started",
            "pipeline_id": pipeline_id,
            "status": "queued",
            "check_status_url": f"/pipeline/status/{pipeline_id}"
        }

    # synchronous
    run_pipeline_background(pipeline_id, request.metadata_path)
    return pipeline_runs[pipeline_id]


@router.get("/pipeline/status/{pipeline_id}")
async def pipeline_status(pipeline_id: str):
    if pipeline_id not in pipeline_runs:
        raise HTTPException(404, f"Pipeline not found: {pipeline_id}")

    return pipeline_runs[pipeline_id]


@router.get("/pipeline/runs")
async def list_pipeline_runs(limit: int = 10):
    runs = list(pipeline_runs.values())
    runs.sort(key=lambda x: x.get("start_time", datetime.min), reverse=True)

    return {"total": len(runs), "runs": runs[:limit]}


@router.delete("/pipeline/{pipeline_id}")
async def cancel_pipeline(pipeline_id: str):
    if pipeline_id not in active_pipelines:
        raise HTTPException(404, f"Pipeline is not running: {pipeline_id}")

    try:
        active_pipelines[pipeline_id].stop()
        del active_pipelines[pipeline_id]

        pipeline_runs[pipeline_id]["status"] = "cancelled"
        pipeline_runs[pipeline_id]["end_time"] = datetime.now()

        return {"message": f"Pipeline {pipeline_id} cancelled"}

    except Exception as e:
        raise HTTPException(500, f"Error cancelling pipeline: {e}")


# --------------------------------------------------------------------
# Logs
# --------------------------------------------------------------------

@router.get("/logs/{pipeline_id}")
async def get_logs(pipeline_id: str):
    log_path = Path(f"/app/logs/pipeline_{pipeline_id}.log")

    if not log_path.exists():
        raise HTTPException(404, f"No logs for pipeline {pipeline_id}")

    try:
        with open(log_path, "r") as f:
            return {"pipeline_id": pipeline_id, "logs": f.read()}
    except Exception as e:
        raise HTTPException(500, f"Error reading logs: {e}")


# --------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------

@router.get("/stats")
async def statistics():
    total = len(pipeline_runs)
    success = sum(1 for r in pipeline_runs.values() if r.get("status") == "success")
    failed = sum(1 for r in pipeline_runs.values() if r.get("status") == "failed")
    running = len(active_pipelines)

    return {
        "total_runs": total,
        "successful_runs": success,
        "failed_runs": failed,
        "running_pipelines": running,
        "success_rate": (success / total * 100) if total > 0 else 0
    }
