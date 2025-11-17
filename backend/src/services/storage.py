"""
Data Storage Module
Handles writing data to various sinks based on metadata configuration
"""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from loguru import logger
import os
from pathlib import Path


class DataStorage:
    """Handles data storage to various sinks"""

    def __init__(self):
        self.write_stats = {}

    def _ensure_directory_exists(self, path: str) -> bool:
        """
        Ensure output directory exists and is writable

        Args:
            path: Directory path to check/create

        Returns:
            bool: True if directory is ready
        """
        try:
            # Handle both local file:// and regular paths
            clean_path = path.replace("file://", "").replace("file:", "")

            # Remove any trailing slashes for consistency
            clean_path = clean_path.rstrip('/')

            # Create parent directories if they don't exist
            directory_path = Path(clean_path)
            directory_path.mkdir(parents=True, exist_ok=True)

            # Set permissions to be world-writable (necessary for Spark temporary files)
            os.chmod(clean_path, 0o777)

            # Verify it's writable
            if not os.access(clean_path, os.W_OK):
                logger.error(f"Directory exists but is not writable: {clean_path}")
                return False

            logger.debug(f"Directory ready and writable: {clean_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to ensure directory exists: {clean_path} - {str(e)}")
            return False

    def write_to_sink(self, df: DataFrame, sink_config: Dict) -> bool:
        """
        Write dataframe to sink based on configuration

        Args:
            df: DataFrame to write
            sink_config: Sink configuration from metadata

        Returns:
            bool: True if successful
        """
        name = sink_config.get("name")
        paths = sink_config.get("paths", [])
        format_type = sink_config.get("format", "JSON").lower()
        save_mode = sink_config.get("saveMode", "OVERWRITE").lower()
        partition_by = sink_config.get("partitionBy", [])

        logger.info(f"Writing to sink: {name} in {format_type} format with mode {save_mode}")

        try:
            record_count = df.count()

            # Ensure all output directories exist before writing
            for path in paths:
                # Extract the base path (remove file:// prefix if present)
                clean_path = path.replace("file://", "").replace("file:", "")

                # For the base output directory
                base_dir = Path(clean_path).parent
                if not self._ensure_directory_exists(str(base_dir)):
                    raise IOError(f"Cannot create/access output directory: {base_dir}")

                # Also ensure the target directory itself exists
                if not self._ensure_directory_exists(clean_path):
                    raise IOError(f"Cannot create/access output directory: {clean_path}")

            for path in paths:
                # Configure Hadoop to use local filesystem with proper permissions
                hadoop_conf = df.sparkSession.sparkContext._jsc.hadoopConfiguration()
                hadoop_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                hadoop_conf.set("fs.default.name", "file:///")

                writer = df.write.format(format_type).mode(save_mode)

                # Apply partitioning if specified
                if partition_by:
                    logger.info(f"Partitioning by: {partition_by}")
                    writer = writer.partitionBy(*partition_by)

                # Write data
                writer.save(path)

                logger.info(f"Successfully wrote {record_count} records to {path}")

            # Store statistics
            self.write_stats[name] = {
                "record_count": record_count,
                "paths": paths,
                "format": format_type,
                "success": True
            }

            return True

        except Exception as e:
            logger.error(f"Error writing to sink {name}: {str(e)}")
            logger.exception("Full traceback:")
            self.write_stats[name] = {
                "error": str(e),
                "success": False
            }
            raise

    def write_multiple_sinks(
        self,
        dataframes: Dict[str, DataFrame],
        sinks: List[Dict]
    ) -> Dict[str, bool]:
        """
        Write multiple dataframes to their respective sinks

        Args:
            dataframes: Dict mapping names to dataframes
            sinks: List of sink configurations

        Returns:
            Dict mapping sink names to success status
        """
        results = {}

        for sink in sinks:
            input_name = sink.get("input")
            sink_name = sink.get("name")

            if input_name not in dataframes:
                logger.error(f"Input dataframe '{input_name}' not found for sink '{sink_name}'")
                results[sink_name] = False
                continue

            df = dataframes[input_name]

            try:
                results[sink_name] = self.write_to_sink(df, sink)
            except Exception as e:
                logger.error(f"Failed to write to sink {sink_name}: {str(e)}")
                results[sink_name] = False

        return results

    def get_write_stats(self) -> Dict:
        """Get statistics about write operations"""
        return self.write_stats

    def validate_sink_config(self, sink_config: Dict) -> bool:
        """
        Validate sink configuration

        Args:
            sink_config: Sink configuration to validate

        Returns:
            bool: True if valid
        """
        required_fields = ["name", "paths", "format"]

        for field in required_fields:
            if field not in sink_config:
                raise ValueError(f"Missing required field in sink config: {field}")

        supported_formats = ["json", "csv", "parquet", "avro"]
        format_type = sink_config["format"].lower()

        if format_type not in supported_formats:
            raise ValueError(f"Unsupported format: {format_type}")

        save_mode = sink_config.get("saveMode", "OVERWRITE").upper()
        valid_modes = ["OVERWRITE", "APPEND", "IGNORE", "ERROR"]

        if save_mode not in valid_modes:
            raise ValueError(f"Invalid save mode: {save_mode}")

        return True

    def create_backup(self, df: DataFrame, backup_path: str) -> bool:
        """
        Create a backup of dataframe

        Args:
            df: DataFrame to backup
            backup_path: Path for backup

        Returns:
            bool: True if successful
        """
        try:
            logger.info(f"Creating backup at {backup_path}")

            # Ensure backup directory exists
            clean_path = backup_path.replace("file://", "").replace("file:", "")
            self._ensure_directory_exists(clean_path)

            df.write.format("parquet").mode("overwrite").save(backup_path)
            return True
        except Exception as e:
            logger.error(f"Failed to create backup: {str(e)}")
            return False