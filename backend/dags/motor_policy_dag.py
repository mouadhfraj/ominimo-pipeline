"""
Airflow DAG for Motor Insurance Policy Pipeline
Orchestrates daily ingestion and processing of motor policy data
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, '/opt/airflow')

from ..src.services.pipeline import MetadataPipeline


# Default arguments for DAG
default_args = {
    'owner': 'ominimo',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['mouad.fraj@ensi-uma.tn'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# Create DAG
dag = DAG(
    'motor_policy_ingestion',
    default_args=default_args,
    description='Daily motor insurance policy data ingestion and validation',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['insurance', 'motor', 'ingestion']
)


def validate_input_files(**context):
    """Validate that input files exist and are readable"""
    input_path = Path("/opt/airflow/data/input/events/motor_policy")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_path}")
    
    files = list(input_path.glob("*.json"))
    
    if not files:
        raise FileNotFoundError(f"No JSON files found in {input_path}")
    
    context['task_instance'].xcom_push(key='file_count', value=len(files))
    
    print(f"Found {len(files)} input files to process")
    return len(files)


def load_metadata(**context):
    """Load and validate metadata configuration"""
    metadata_path = "/opt/airflow/metadata/motor_policy.json"
    
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Validate metadata structure
    if "dataflows" not in metadata:
        raise ValueError("Invalid metadata: missing 'dataflows'")
    
    dataflow = metadata["dataflows"][0]
    context['task_instance'].xcom_push(key='dataflow_name', value=dataflow.get('name'))
    
    print(f"Loaded metadata for dataflow: {dataflow.get('name')}")
    return metadata_path


def run_data_ingestion(**context):
    """Execute data ingestion stage"""
    metadata_path = context['task_instance'].xcom_pull(task_ids='load_metadata')
    
    pipeline = MetadataPipeline(metadata_path)
    
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        dataflow = metadata["dataflows"][0]
        pipeline.execute_ingestion(dataflow)
        
        # Store record counts in XCom
        record_counts = {
            name: df.count() 
            for name, df in pipeline.dataframes.items()
        }
        
        context['task_instance'].xcom_push(key='ingestion_counts', value=record_counts)
        
        print(f"Ingestion complete: {record_counts}")
        return record_counts
    finally:
        pipeline.stop()


def run_data_validation(**context):
    """Execute data validation stage"""
    metadata_path = context['task_instance'].xcom_pull(task_ids='load_metadata')
    
    pipeline = MetadataPipeline(metadata_path)
    
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        dataflow = metadata["dataflows"][0]
        
        # Re-run ingestion (in production, you'd load from previous stage)
        pipeline.execute_ingestion(dataflow)
        pipeline.execute_transformations(dataflow)
        
        # Extract validation statistics
        valid_count = pipeline.dataframes.get("validation_ok", None)
        invalid_count = pipeline.dataframes.get("validation_ko", None)
        
        validation_stats = {
            "valid_records": valid_count.count() if valid_count else 0,
            "invalid_records": invalid_count.count() if invalid_count else 0
        }
        
        context['task_instance'].xcom_push(key='validation_stats', value=validation_stats)
        
        print(f"Validation complete: {validation_stats}")
        return validation_stats
    finally:
        pipeline.stop()


def run_data_storage(**context):
    """Execute data storage stage"""
    metadata_path = context['task_instance'].xcom_pull(task_ids='load_metadata')
    
    pipeline = MetadataPipeline(metadata_path)
    
    try:
        # Execute full pipeline
        stats = pipeline.execute()
        
        context['task_instance'].xcom_push(key='pipeline_stats', value=stats)
        
        print(f"Storage complete. Pipeline status: {stats['status']}")
        return stats
    finally:
        pipeline.stop()


def generate_report(**context):
    """Generate pipeline execution report"""
    ingestion_counts = context['task_instance'].xcom_pull(
        task_ids='run_ingestion', 
        key='ingestion_counts'
    )
    validation_stats = context['task_instance'].xcom_pull(
        task_ids='run_validation', 
        key='validation_stats'
    )
    pipeline_stats = context['task_instance'].xcom_pull(
        task_ids='run_storage', 
        key='pipeline_stats'
    )
    
    report = {
        "execution_date": context['execution_date'].isoformat(),
        "dag_run_id": context['dag_run'].run_id,
        "ingestion": ingestion_counts,
        "validation": validation_stats,
        "pipeline": pipeline_stats
    }
    
    # Save report
    report_path = Path(f"/opt/airflow/logs/report_{context['execution_date'].strftime('%Y%m%d')}.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"Report generated: {report_path}")
    print(json.dumps(report, indent=2, default=str))
    
    return report


def cleanup_old_files(**context):
    """Clean up old processed files (older than 7 days)"""
    from datetime import datetime, timedelta
    
    cutoff_date = datetime.now() - timedelta(days=7)
    
    directories = [
        Path("/opt/airflow/data/output/events/motor_policy"),
        Path("/opt/airflow/data/output/discards/motor_policy")
    ]
    
    deleted_count = 0
    
    for directory in directories:
        if directory.exists():
            for file_path in directory.rglob("*"):
                if file_path.is_file():
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_date:
                        file_path.unlink()
                        deleted_count += 1
    
    print(f"Cleaned up {deleted_count} old files")
    return deleted_count


# Define tasks
validate_input = PythonOperator(
    task_id='validate_input',
    python_callable=validate_input_files,
    provide_context=True,
    dag=dag
)

load_metadata_task = PythonOperator(
    task_id='load_metadata',
    python_callable=load_metadata,
    provide_context=True,
    dag=dag
)

run_ingestion = PythonOperator(
    task_id='run_ingestion',
    python_callable=run_data_ingestion,
    provide_context=True,
    dag=dag
)

run_validation = PythonOperator(
    task_id='run_validation',
    python_callable=run_data_validation,
    provide_context=True,
    dag=dag
)

run_storage = PythonOperator(
    task_id='run_storage',
    python_callable=run_data_storage,
    provide_context=True,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_old_files,
    provide_context=True,
    dag=dag
)

# Define task dependencies
validate_input >> load_metadata_task >> run_ingestion >> run_validation >> run_storage >> generate_report_task >> cleanup_task