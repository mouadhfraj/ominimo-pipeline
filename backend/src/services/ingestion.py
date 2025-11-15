"""
Data Ingestion Module
Reads data from various sources based on metadata configuration
"""

from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from loguru import logger


class DataIngestion:
    """Handles data ingestion from multiple sources"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.dataframes: Dict[str, DataFrame] = {}
    
    def read_source(self, source_config: Dict) -> DataFrame:
        """
        Read data from a source based on configuration
        
        Args:
            source_config: Source configuration from metadata
            
        Returns:
            DataFrame: Loaded data
        """
        name = source_config.get("name")
        path = source_config.get("path")
        format_type = source_config.get("format", "JSON").lower()
        options = source_config.get("options", {})
        
        logger.info(f"Reading source: {name} from {path} in {format_type} format")
        
        try:
            reader = self.spark.read.format(format_type)
            
            # Apply options
            for key, value in options.items():
                reader = reader.option(key, value)
            
            df = reader.load(path)
            
            logger.info(f"Successfully loaded {df.count()} records from {name}")
            self.dataframes[name] = df
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading source {name}: {str(e)}")
            raise
    
    def read_all_sources(self, sources: List[Dict]) -> Dict[str, DataFrame]:
        """
        Read all sources defined in metadata
        
        Args:
            sources: List of source configurations
            
        Returns:
            Dict[str, DataFrame]: Dictionary of loaded dataframes
        """
        for source in sources:
            self.read_source(source)
        
        return self.dataframes
    
    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """
        Get a loaded dataframe by name
        
        Args:
            name: Name of the dataframe
            
        Returns:
            DataFrame or None if not found
        """
        return self.dataframes.get(name)
    
    def validate_source_config(self, source_config: Dict) -> bool:
        """
        Validate source configuration
        
        Args:
            source_config: Source configuration to validate
            
        Returns:
            bool: True if valid, raises exception otherwise
        """
        required_fields = ["name", "path", "format"]
        
        for field in required_fields:
            if field not in source_config:
                raise ValueError(f"Missing required field in source config: {field}")
        
        supported_formats = ["json", "csv", "parquet", "avro"]
        format_type = source_config["format"].lower()
        
        if format_type not in supported_formats:
            raise ValueError(f"Unsupported format: {format_type}. Supported: {supported_formats}")
        
        return True