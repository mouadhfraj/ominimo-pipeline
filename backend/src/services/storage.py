"""
Data Storage Module
Handles writing data to various sinks based on metadata configuration
"""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame
from loguru import logger


class DataStorage:
    """Handles data storage to various sinks"""
    
    def __init__(self):
        self.write_stats = {}
    
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
            
            for path in paths:
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
            df.write.format("parquet").mode("overwrite").save(backup_path)
            return True
        except Exception as e:
            logger.error(f"Failed to create backup: {str(e)}")
            return False