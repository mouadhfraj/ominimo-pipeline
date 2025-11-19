"""
Repository layer for database operations
Provides high-level interface for CRUD operations
"""

from typing import List, Optional, Dict
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_
from loguru import logger

from .models import MetadataFile, PipelineRun, PipelineLog


class MetadataRepository:
    """Repository for metadata file operations"""

    @staticmethod
    def create(db: Session, name: str, version: str, content: Dict, description: str = None) -> MetadataFile:
        """Create new metadata file"""
        metadata_file  = MetadataFile(
            name=name,
            version=version,
            description=description,
            content=content,
            is_active=True
        )
        db.add(metadata_file )
        db.commit()
        db.refresh(metadata_file )
        logger.info(f"Created metadata: {name} v{version}")
        return metadata_file

    @staticmethod
    def get_by_name(db: Session, name: str) -> Optional[MetadataFile]:
        """Get metadata by name"""
        return db.query(MetadataFile).filter(MetadataFile.name == name).first()

    @staticmethod
    def get_all(db: Session, active_only: bool = True) -> List[MetadataFile]:
        """Get all metadata files"""
        query = db.query(MetadataFile)
        if active_only:
            query = query.filter(MetadataFile.is_active == True)
        return query.order_by(desc(MetadataFile.updated_at)).all()

    @staticmethod
    def update(db: Session, name: str, version: str = None, content: Dict = None, description: str = None) -> Optional[MetadataFile]:
        """Update metadata file"""
        metadata = MetadataRepository.get_by_name(db, name)
        if not metadata:
            return None

        if version:
            metadata.version = version
        if content:
            metadata.content = content
        if description is not None:
            metadata.description = description

        metadata.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(metadata)
        logger.info(f"Updated metadata: {name}")
        return metadata

    @staticmethod
    def delete(db: Session, name: str, soft: bool = True) -> bool:
        """Delete metadata (soft or hard)"""
        metadata = MetadataRepository.get_by_name(db, name)
        if not metadata:
            return False

        if soft:
            metadata.is_active = False
            db.commit()
            logger.info(f"Soft deleted metadata: {name}")
        else:
            db.delete(metadata)
            db.commit()
            logger.info(f"Hard deleted metadata: {name}")

        return True


class PipelineRunRepository:
    """Repository for pipeline run operations"""

    @staticmethod
    def create(
        db: Session,
        pipeline_id: str,
        metadata_name: str,
        status: str = "queued"
    ) -> PipelineRun:
        """Create new pipeline run"""
        run = PipelineRun(
            pipeline_id=pipeline_id,
            metadata_name=metadata_name,
            status=status,
            start_time=datetime.utcnow()
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        logger.info(f"Created pipeline run: {pipeline_id}")
        return run

    @staticmethod
    def get_by_id(db: Session, pipeline_id: str) -> Optional[PipelineRun]:
        """Get pipeline run by ID"""
        return db.query(PipelineRun).filter(PipelineRun.pipeline_id == pipeline_id).first()

    @staticmethod
    def get_all(
        db: Session,
        status: str = None,
        metadata_name: str = None,
        limit: int = 100
    ) -> List[PipelineRun]:
        """Get all pipeline runs with optional filters"""
        query = db.query(PipelineRun)

        if status:
            query = query.filter(PipelineRun.status == status)
        if metadata_name:
            query = query.filter(PipelineRun.metadata_name == metadata_name)

        return query.order_by(desc(PipelineRun.start_time)).limit(limit).all()

    @staticmethod
    def update_status(
        db: Session,
        pipeline_id: str,
        status: str,
        end_time: datetime = None,
        error_message: str = None
    ) -> Optional[PipelineRun]:
        """Update pipeline run status"""
        run = PipelineRunRepository.get_by_id(db, pipeline_id)
        if not run:
            return None

        run.status = status
        if end_time:
            run.end_time = end_time
            if run.start_time:
                run.duration_seconds = (end_time - run.start_time).total_seconds()
        if error_message:
            run.error_message = error_message

        db.commit()
        db.refresh(run)
        logger.info(f"Updated pipeline run {pipeline_id} status to {status}")
        return run

    @staticmethod
    def update_metrics(
        db: Session,
        pipeline_id: str,
        total_records: int = None,
        valid_records: int = None,
        invalid_records: int = None,
        stages: Dict = None
    ) -> Optional[PipelineRun]:
        """Update pipeline run metrics"""
        run = PipelineRunRepository.get_by_id(db, pipeline_id)
        if not run:
            return None

        if total_records is not None:
            run.total_records = total_records
        if valid_records is not None:
            run.valid_records = valid_records
        if invalid_records is not None:
            run.invalid_records = invalid_records

        if valid_records is not None and total_records is not None and total_records > 0:
            run.valid_percentage = (valid_records / total_records) * 100

        if stages:
            run.stages = stages

        db.commit()
        db.refresh(run)
        return run

    @staticmethod
    def get_statistics(db: Session) -> Dict:
        """Get pipeline execution statistics"""
        total = db.query(PipelineRun).count()
        success = db.query(PipelineRun).filter(PipelineRun.status == "success").count()
        failed = db.query(PipelineRun).filter(PipelineRun.status == "failed").count()
        running = db.query(PipelineRun).filter(PipelineRun.status == "running").count()

        return {
            "total_runs": total,
            "successful_runs": success,
            "failed_runs": failed,
            "running_pipelines": running,
            "success_rate": (success / total * 100) if total > 0 else 0
        }


class PipelineLogRepository:
    """Repository for pipeline log operations"""

    @staticmethod
    def create(
        db: Session,
        pipeline_id: str,
        level: str,
        message: str,
        stage: str = None,
        details: Dict = None
    ) -> PipelineLog:
        """Create new log entry"""
        log = PipelineLog(
            pipeline_id=pipeline_id,
            level=level,
            message=message,
            stage=stage,
            details=details,
            timestamp=datetime.utcnow()
        )
        db.add(log)
        db.commit()
        db.refresh(log)
        return log

    @staticmethod
    def create_batch(db: Session, logs: List[Dict]) -> int:
        """Create multiple log entries"""
        log_objects = [
            PipelineLog(
                pipeline_id=log["pipeline_id"],
                level=log["level"],
                message=log["message"],
                stage=log.get("stage"),
                details=log.get("details"),
                timestamp=log.get("timestamp", datetime.utcnow())
            )
            for log in logs
        ]
        db.add_all(log_objects)
        db.commit()
        return len(log_objects)

    @staticmethod
    def get_by_pipeline(
        db: Session,
        pipeline_id: str,
        level: str = None,
        limit: int = 1000
    ) -> List[PipelineLog]:
        """Get logs for a pipeline run"""
        query = db.query(PipelineLog).filter(PipelineLog.pipeline_id == pipeline_id)

        if level:
            query = query.filter(PipelineLog.level == level)

        return query.order_by(desc(PipelineLog.timestamp)).limit(limit).all()

    @staticmethod
    def get_recent_errors(db: Session, limit: int = 100) -> List[PipelineLog]:
        """Get recent error logs"""
        return (
            db.query(PipelineLog)
            .filter(or_(PipelineLog.level == "ERROR", PipelineLog.level == "CRITICAL"))
            .order_by(desc(PipelineLog.timestamp))
            .limit(limit)
            .all()
        )

    @staticmethod
    def delete_old_logs(db: Session, days: int = 30) -> int:
        """Delete logs older than specified days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        deleted = db.query(PipelineLog).filter(PipelineLog.timestamp < cutoff_date).delete()
        db.commit()
        logger.info(f"Deleted {deleted} old log entries")
        return deleted