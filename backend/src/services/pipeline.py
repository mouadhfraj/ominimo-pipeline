"""
Pipeline Orchestration Module - Fully Metadata-Driven
Dynamically executes ANY pipeline defined in metadata without code changes
"""

import json
from typing import Dict, Optional, List
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from loguru import logger

from ingestion import DataIngestion
from validation import DataValidator
from transformation import DataTransformer
from storage import DataStorage


class MetadataPipeline:
    """
    Fully metadata-driven pipeline orchestrator
    ALL logic comes from metadata - ZERO hardcoded business logic
    """
    
    def __init__(self, metadata_path: str, spark: Optional[SparkSession] = None):
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()
        self.spark = spark or self._create_spark_session()
        
        # Initialize components
        self.ingestion = DataIngestion(self.spark)
        self.validator = DataValidator(self.spark)
        self.transformer = DataTransformer(self.spark)
        self.storage = DataStorage()
        
        # Pipeline state - stores all intermediate dataframes
        self.dataframes: Dict[str, DataFrame] = {}
        self.execution_stats = {
            "start_time": None,
            "end_time": None,
            "status": "initialized",
            "stages": {},
            "metadata_version": self.metadata.get("version", "unknown")
        }
    
    def _load_metadata(self) -> Dict:
        """Load and validate metadata configuration"""
        logger.info(f"Loading metadata from {self.metadata_path}")
        
        try:
            with open(self.metadata_path, 'r') as f:
                metadata = json.load(f)
            
            # Validate metadata structure
            if "dataflows" not in metadata:
                raise ValueError("Invalid metadata: missing 'dataflows' key")
            
            if not isinstance(metadata["dataflows"], list) or len(metadata["dataflows"]) == 0:
                raise ValueError("Invalid metadata: 'dataflows' must be a non-empty list")
            
            logger.info("Metadata loaded and validated successfully")
            return metadata
        except Exception as e:
            logger.error(f"Failed to load metadata: {str(e)}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session from metadata configuration"""
        dataflow = self.metadata["dataflows"][0]
        settings = dataflow.get("settings", {})
        spark_config = settings.get("spark", {})
        
        app_name = spark_config.get("app_name", "metadata-driven-pipeline")
        master = spark_config.get("master", "local[*]")
        configs = spark_config.get("config", {})
        
        logger.info(f"Creating Spark session: {app_name}")
        
        builder = SparkSession.builder.appName(app_name).master(master)
        
        # Apply all configs from metadata
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    
    def execute_ingestion(self, dataflow: Dict) -> None:
        """
        Execute data ingestion stage - fully dynamic
        Reads ALL sources defined in metadata
        """
        logger.info("=" * 80)
        logger.info("STAGE 1: DATA INGESTION")
        logger.info("=" * 80)
        
        self.execution_stats["stages"]["ingestion"] = {
            "start_time": datetime.now(),
            "sources": []
        }
        
        try:
            sources = dataflow.get("sources", [])
            
            if not sources:
                logger.warning("No sources defined in metadata")
                return
            
            logger.info(f"Processing {len(sources)} source(s)")
            
            # Load each source
            for source in sources:
                source_name = source["name"]
                logger.info(f"Loading source: {source_name}")
                
                df = self.ingestion.read_source(source)
                self.dataframes[source_name] = df
                
                # Track statistics
                record_count = df.count()
                self.execution_stats["stages"]["ingestion"]["sources"].append({
                    "name": source_name,
                    "record_count": record_count,
                    "path": source.get("path"),
                    "format": source.get("format")
                })
            
            self.execution_stats["stages"]["ingestion"]["status"] = "success"
            logger.info(f"✓ Ingestion complete: {len(self.dataframes)} dataframe(s) loaded")
            
        except Exception as e:
            self.execution_stats["stages"]["ingestion"]["status"] = "failed"
            self.execution_stats["stages"]["ingestion"]["error"] = str(e)
            logger.error(f"✗ Ingestion failed: {str(e)}")
            raise
        finally:
            self.execution_stats["stages"]["ingestion"]["end_time"] = datetime.now()
    
    def execute_transformations(self, dataflow: Dict) -> None:
        """
        Execute transformation stage - fully dynamic
        Applies ALL transformations defined in metadata in sequence
        """
        logger.info("=" * 80)
        logger.info("STAGE 2: TRANSFORMATIONS")
        logger.info("=" * 80)
        
        self.execution_stats["stages"]["transformation"] = {
            "start_time": datetime.now(),
            "transformations": []
        }
        
        try:
            transformations = dataflow.get("transformations", [])
            
            if not transformations:
                logger.warning("No transformations defined in metadata")
                return
            
            logger.info(f"Processing {len(transformations)} transformation(s)")
            
            # Execute each transformation
            for idx, transform in enumerate(transformations, 1):
                transform_name = transform.get("name", f"transform_{idx}")
                transform_type = transform["type"]
                params = transform.get("params", {})
                input_name = params.get("input")
                
                logger.info(f"[{idx}/{len(transformations)}] Executing: {transform_name} (type: {transform_type})")
                
                # Get input dataframe
                if input_name not in self.dataframes:
                    raise ValueError(f"Input dataframe '{input_name}' not found. Available: {list(self.dataframes.keys())}")
                
                input_df = self.dataframes[input_name]
                input_count = input_df.count()
                
                # Route to appropriate handler based on type
                if transform_type == "validate_fields":
                    output_dfs = self._execute_validation(transform_name, input_df, params)
                    # Validation produces multiple outputs
                    for output_name, output_df in output_dfs.items():
                        self.dataframes[output_name] = output_df
                    
                else:
                    # All other transformations
                    output_df = self.transformer.apply_transformations(input_df, transform)
                    output_count = output_df.count()
                    
                    # Store output - use transform name as key
                    self.dataframes[transform_name] = output_df
                    
                    logger.info(f"  → Output: {output_count} records")
                
                # Track transformation stats
                self.execution_stats["stages"]["transformation"]["transformations"].append({
                    "name": transform_name,
                    "type": transform_type,
                    "input": input_name,
                    "input_count": input_count
                })
            
            self.execution_stats["stages"]["transformation"]["status"] = "success"
            logger.info(f"✓ Transformations complete")
            
        except Exception as e:
            self.execution_stats["stages"]["transformation"]["status"] = "failed"
            self.execution_stats["stages"]["transformation"]["error"] = str(e)
            logger.error(f"✗ Transformation failed: {str(e)}")
            raise
        finally:
            self.execution_stats["stages"]["transformation"]["end_time"] = datetime.now()
    
    def _execute_validation(
        self, 
        transform_name: str, 
        input_df: DataFrame, 
        params: Dict
    ) -> Dict[str, DataFrame]:
        """
        Execute validation transformation
        Returns dict with both valid and invalid dataframes
        """
        valid_df, invalid_df = self.validator.validate_dataframe(input_df, params)
        
        # Determine output names from metadata or use defaults
        valid_output = params.get("valid_output", "validation_ok")
        invalid_output = params.get("invalid_output", "validation_ko")
        
        logger.info(f"  → Valid output: {valid_output} ({valid_df.count()} records)")
        logger.info(f"  → Invalid output: {invalid_output} ({invalid_df.count()} records)")
        
        return {
            valid_output: valid_df,
            invalid_output: invalid_df
        }
    
    def execute_storage(self, dataflow: Dict) -> None:
        """
        Execute storage stage - fully dynamic
        Writes ALL sinks defined in metadata
        """
        logger.info("=" * 80)
        logger.info("STAGE 3: DATA STORAGE")
        logger.info("=" * 80)
        
        self.execution_stats["stages"]["storage"] = {
            "start_time": datetime.now(),
            "sinks": []
        }
        
        try:
            sinks = dataflow.get("sinks", [])
            
            if not sinks:
                logger.warning("No sinks defined in metadata")
                return
            
            logger.info(f"Processing {len(sinks)} sink(s)")
            
            # Write to each sink
            for idx, sink in enumerate(sinks, 1):
                sink_name = sink.get("name", f"sink_{idx}")
                input_name = sink.get("input")
                
                logger.info(f"[{idx}/{len(sinks)}] Writing to sink: {sink_name}")
                
                # Get input dataframe
                if input_name not in self.dataframes:
                    raise ValueError(f"Input dataframe '{input_name}' not found for sink '{sink_name}'. Available: {list(self.dataframes.keys())}")
                
                input_df = self.dataframes[input_name]
                
                # Write to sink
                success = self.storage.write_to_sink(input_df, sink)
                
                # Track statistics
                write_stats = self.storage.write_stats.get(sink_name, {})
                self.execution_stats["stages"]["storage"]["sinks"].append({
                    "name": sink_name,
                    "input": input_name,
                    "success": success,
                    "record_count": write_stats.get("record_count", 0),
                    "paths": write_stats.get("paths", [])
                })
                
                logger.info(f"  → Success: {success}")
            
            self.execution_stats["stages"]["storage"]["status"] = "success"
            logger.info(f"✓ Storage complete: {len(sinks)} sink(s) written")
            
        except Exception as e:
            self.execution_stats["stages"]["storage"]["status"] = "failed"
            self.execution_stats["stages"]["storage"]["error"] = str(e)
            logger.error(f"✗ Storage failed: {str(e)}")
            raise
        finally:
            self.execution_stats["stages"]["storage"]["end_time"] = datetime.now()
    
    def execute(self) -> Dict:
        """
        Execute complete pipeline - fully metadata-driven
        NO hardcoded logic - everything comes from metadata
        
        Returns:
            Dict: Execution statistics
        """
        logger.info("=" * 80)
        logger.info("METADATA-DRIVEN PIPELINE EXECUTION")
        logger.info(f"Metadata: {self.metadata_path}")
        logger.info("=" * 80)
        
        self.execution_stats["start_time"] = datetime.now()
        self.execution_stats["status"] = "running"
        
        try:
            # Get dataflow configuration
            dataflow = self.metadata["dataflows"][0]
            dataflow_name = dataflow.get("name", "unnamed")
            dataflow_version = dataflow.get("version", "unknown")
            
            logger.info(f"Dataflow: {dataflow_name} (v{dataflow_version})")
            logger.info(f"Description: {dataflow.get('description', 'N/A')}")
            
            # Execute pipeline stages in order
            self.execute_ingestion(dataflow)
            self.execute_transformations(dataflow)
            self.execute_storage(dataflow)
            
            self.execution_stats["status"] = "success"
            logger.info("=" * 80)
            logger.info("✓ PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
        
        except Exception as e:
            self.execution_stats["status"] = "failed"
            self.execution_stats["error"] = str(e)
            logger.error("=" * 80)
            logger.error("✗ PIPELINE EXECUTION FAILED")
            logger.error(f"Error: {str(e)}")
            logger.error("=" * 80)
            raise
        
        finally:
            self.execution_stats["end_time"] = datetime.now()
            duration = (self.execution_stats["end_time"] - self.execution_stats["start_time"]).total_seconds()
            self.execution_stats["duration_seconds"] = duration
            
            logger.info(f"Total Duration: {duration:.2f} seconds")
            self._print_execution_summary()
        
        return self.execution_stats
    
    def _print_execution_summary(self):
        """Print execution summary"""
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        for stage_name, stage_stats in self.execution_stats.get("stages", {}).items():
            status = stage_stats.get("status", "unknown")
            status_symbol = "✓" if status == "success" else "✗"
            logger.info(f"{status_symbol} {stage_name.upper()}: {status}")
            
            if stage_name == "ingestion":
                sources = stage_stats.get("sources", [])
                total_records = sum(s.get("record_count", 0) for s in sources)
                logger.info(f"  → {len(sources)} source(s), {total_records} total records")
            
            elif stage_name == "transformation":
                transforms = stage_stats.get("transformations", [])
                logger.info(f"  → {len(transforms)} transformation(s) applied")
            
            elif stage_name == "storage":
                sinks = stage_stats.get("sinks", [])
                successful = sum(1 for s in sinks if s.get("success"))
                logger.info(f"  → {successful}/{len(sinks)} sink(s) written successfully")
        
        logger.info("=" * 80)
    
    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Get a dataframe by name"""
        return self.dataframes.get(name)
    
    def list_dataframes(self) -> List[str]:
        """List all available dataframe names"""
        return list(self.dataframes.keys())
    
    def get_execution_stats(self) -> Dict:
        """Get execution statistics"""
        return self.execution_stats
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            logger.info("Stopping Spark session")
            self.spark.stop()


def run_pipeline(metadata_path: str) -> Dict:
    """
    Convenience function to run pipeline from metadata
    
    Args:
        metadata_path: Path to metadata JSON file
        
    Returns:
        Dict: Execution statistics
    """
    pipeline = MetadataPipeline(metadata_path)
    
    try:
        stats = pipeline.execute()
        return stats
    finally:
        pipeline.stop()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <metadata_path>")
        print("\nExample:")
        print("  python pipeline.py metadata/motor_policy.json")
        sys.exit(1)
    
    metadata_path = sys.argv[1]
    
    if not Path(metadata_path).exists():
        print(f"Error: Metadata file not found: {metadata_path}")
        sys.exit(1)
    
    print(f"\nExecuting pipeline from metadata: {metadata_path}\n")
    
    stats = run_pipeline(metadata_path)
    
    print("\n" + "=" * 80)
    print("EXECUTION STATISTICS")
    print("=" * 80)
    print(json.dumps(stats, indent=2, default=str))