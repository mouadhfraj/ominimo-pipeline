"""
SQLAlchemy models for pipeline database
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Text, Boolean, Float, DateTime, ForeignKey, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .database import Base


class MetadataFile(Base):
    __tablename__ = "metadata_files"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    version = Column(String(50), nullable=False)
    description = Column(Text)
    content = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True, index=True)


    pipeline_runs = relationship("PipelineRun", back_populates="metadata_file", cascade="all, delete-orphan")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(String(255), unique=True, nullable=False)
    metadata_name = Column(String(255), ForeignKey("metadata_files.name"), nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime)
    duration_seconds = Column(Float)
    total_records = Column(Integer, default=0)
    valid_records = Column(Integer, default=0)
    invalid_records = Column(Integer, default=0)
    valid_percentage = Column(Float)
    stages = Column(JSONB)
    error_message = Column(Text)


    metadata_file = relationship("MetadataFile", back_populates="pipeline_runs")
    logs = relationship("PipelineLog", back_populates="pipeline_run", cascade="all, delete-orphan")

class PipelineLog(Base):
    """Stores detailed execution logs"""
    __tablename__ = "pipeline_logs"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(String(255), ForeignKey("pipeline_runs.pipeline_id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    level = Column(String(20), nullable=False, index=True)
    stage = Column(String(100))
    message = Column(Text, nullable=False)
    details = Column(JSONB)


    pipeline_run = relationship("PipelineRun", back_populates="logs")


    __table_args__ = (
        CheckConstraint(
            "level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')",
            name="check_log_level"
        ),
    )

    def __repr__(self):
        return f"<PipelineLog(pipeline_id='{self.pipeline_id}', level='{self.level}')>"