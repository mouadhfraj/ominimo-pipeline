"""
Airflow DAG for Motor Insurance Policy Pipeline - Database Integrated
Orchestrates daily ingestion and processing of motor policy data
Now uses database metadata instead of file system
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the source directory to Python path
sys.path.insert(0, '/opt/airflow/src')
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/backend')

# Import using absolute imports
from src.services.pipeline import MetadataPipeline
from src.repo import get_db_context, MetadataRepository


# Configuration
METADATA_NAME = "motor_policy_airflow"  # Name in database, not file path


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
    description='Daily motor insurance policy data ingestion and validation (Database-driven)',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['insurance', 'motor', 'ingestion', 'database']
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

    print(f"✓ Found {len(files)} input files to process")
    for f in files:
        print(f"  - {f.name}")

    return len(files)


def load_metadata_from_database(**context):
    """Load and validate metadata configuration from database"""
    print(f"Loading metadata '{METADATA_NAME}' from database...")

    with get_db_context() as db:
        metadata_file = MetadataRepository.get_by_name(db, METADATA_NAME)

        if not metadata_file:
            raise ValueError(f"Metadata '{METADATA_NAME}' not found in database")

        if not metadata_file.is_active:
            raise ValueError(f"Metadata '{METADATA_NAME}' is not active")

        metadata = metadata_file.content

        # Validate metadata structure
        if "dataflows" not in metadata:
            raise ValueError("Invalid metadata: missing 'dataflows'")

        dataflow = metadata["dataflows"][0]

        # Push to XCom
        context['task_instance'].xcom_push(key='dataflow_name', value=dataflow.get('name'))
        context['task_instance'].xcom_push(key='metadata_name', value=METADATA_NAME)
        context['task_instance'].xcom_push(key='metadata_version', value=metadata_file.version)

        print(f"✓ Loaded metadata from database: {METADATA_NAME}")
        print(f"  Version: {metadata_file.version}")
        print(f"  Description: {metadata_file.description}")
        print(f"  Sources: {len(dataflow.get('sources', []))}")
        print(f"  Transformations: {len(dataflow.get('transformations', []))}")
        print(f"  Sinks: {len(dataflow.get('sinks', []))}")

        return METADATA_NAME


def run_full_pipeline(**context):
    """Execute complete pipeline - ingestion, transformation, and storage"""
    metadata_name = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_name'
    )

    metadata_version = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_version'
    )

    print(f"Executing pipeline with metadata from database: {metadata_name} v{metadata_version}")

    # Create pipeline using metadata NAME (not path)
    pipeline = MetadataPipeline(metadata_name)

    try:
        # Execute full pipeline
        stats = pipeline.execute()

        # Extract key metrics
        ingestion_stats = stats.get('stages', {}).get('ingestion', {})
        transformation_stats = stats.get('stages', {}).get('transformation', {})
        storage_stats = stats.get('stages', {}).get('storage', {})

        # Calculate validation metrics
        valid_count = 0
        invalid_count = 0

        # Try to get actual counts from dataframes
        if 'validation_ok' in pipeline.dataframes:
            valid_count = pipeline.dataframes['validation_ok'].count()
        if 'validation_ko' in pipeline.dataframes:
            invalid_count = pipeline.dataframes['validation_ko'].count()

        # Store in XCom
        context['task_instance'].xcom_push(key='pipeline_stats', value=stats)
        context['task_instance'].xcom_push(key='valid_count', value=valid_count)
        context['task_instance'].xcom_push(key='invalid_count', value=invalid_count)

        print(f"\n{'='*80}")
        print(f"Pipeline Execution Summary")
        print(f"{'='*80}")
        print(f"Metadata: {metadata_name} v{metadata_version}")
        print(f"Status: {stats['status']}")
        print(f"Duration: {stats.get('duration_seconds', 0):.2f} seconds")
        print(f"Valid Records: {valid_count}")
        print(f"Invalid Records: {invalid_count}")
        print(f"{'='*80}\n")

        # Check if pipeline met quality standards
        if stats['status'] != 'success':
            raise RuntimeError(f"Pipeline failed with status: {stats['status']}")

        return stats

    except Exception as e:
        print(f"✗ Pipeline execution failed: {str(e)}")
        raise
    finally:
        pipeline.stop()


def check_quality_metrics(**context):
    """Check if pipeline met quality standards"""
    valid_count = context['task_instance'].xcom_pull(
        task_ids='run_pipeline',
        key='valid_count'
    )
    invalid_count = context['task_instance'].xcom_pull(
        task_ids='run_pipeline',
        key='invalid_count'
    )

    metadata_name = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_name'
    )

    total_count = valid_count + invalid_count

    if total_count == 0:
        raise ValueError("No records processed!")

    valid_percentage = (valid_count / total_count) * 100

    # Load quality rules from database metadata
    with get_db_context() as db:
        metadata_file = MetadataRepository.get_by_name(db, metadata_name)
        metadata = metadata_file.content

    quality_rules = metadata['dataflows'][0].get('settings', {}).get('quality_rules', {})
    min_valid_percentage = quality_rules.get('min_valid_record_percentage', 80)

    print(f"\n{'='*80}")
    print(f"Quality Metrics Check")
    print(f"{'='*80}")
    print(f"Total Records: {total_count}")
    print(f"Valid Records: {valid_count} ({valid_percentage:.2f}%)")
    print(f"Invalid Records: {invalid_count} ({100-valid_percentage:.2f}%)")
    print(f"Required Minimum: {min_valid_percentage}%")

    if valid_percentage < min_valid_percentage:
        print(f"✗ FAILED: Valid percentage {valid_percentage:.2f}% is below minimum {min_valid_percentage}%")
        print(f"{'='*80}\n")
        raise ValueError(
            f"Quality check failed: {valid_percentage:.2f}% valid records "
            f"(minimum: {min_valid_percentage}%)"
        )

    print(f"✓ PASSED: Quality standards met")
    print(f"{'='*80}\n")

    return {
        "total": total_count,
        "valid": valid_count,
        "invalid": invalid_count,
        "valid_percentage": valid_percentage,
        "quality_check": "PASSED"
    }


def generate_report(**context):
    """Generate pipeline execution report"""
    pipeline_stats = context['task_instance'].xcom_pull(
        task_ids='run_pipeline',
        key='pipeline_stats'
    )
    quality_metrics = context['task_instance'].xcom_pull(
        task_ids='check_quality'
    )

    metadata_name = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_name'
    )

    metadata_version = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_version'
    )

    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id

    report = {
        "execution_date": execution_date.isoformat(),
        "dag_run_id": dag_run_id,
        "metadata_name": metadata_name,
        "metadata_version": metadata_version,
        "pipeline_id": pipeline_stats.get('pipeline_id', 'unknown'),
        "status": pipeline_stats.get('status'),
        "duration_seconds": pipeline_stats.get('duration_seconds'),
        "quality_metrics": quality_metrics,
        "stages": pipeline_stats.get('stages', {}),
        "source": "database"
    }

    # Save report
    report_dir = Path("/opt/airflow/logs/reports")
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / f"motor_policy_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'='*80}")
    print(f"Execution Report")
    print(f"{'='*80}")
    print(json.dumps(report, indent=2, default=str))
    print(f"{'='*80}")
    print(f"Report saved to: {report_path}")
    print(f"{'='*80}\n")

    return report


def send_notification(**context):
    """Send notification (email/slack) about pipeline execution"""
    report = context['task_instance'].xcom_pull(task_ids='generate_report')

    status = report['status']
    quality = report['quality_metrics']

    # Format notification message
    message = f"""
Motor Policy Pipeline Execution Report
{'='*50}

Status: {status.upper()}
Execution Date: {report['execution_date']}
Duration: {report['duration_seconds']:.2f} seconds

Metadata:
  Name: {report['metadata_name']}
  Version: {report['metadata_version']}
  Source: Database

Data Quality:
  Total Records: {quality['total']}
  Valid Records: {quality['valid']} ({quality['valid_percentage']:.2f}%)
  Invalid Records: {quality['invalid']}
  Quality Check: {quality['quality_check']}

Pipeline ID: {report['pipeline_id']}
"""

    print(message)

    # In production, send via email or Slack
    # For now, just log
    print("✓ Notification logged (configure email/Slack for actual notifications)")

    return message


def cleanup_old_files(**context):
    """Clean up old processed files (older than 7 days)"""
    cutoff_date = datetime.now() - timedelta(days=7)

    directories = [
        Path("/opt/airflow/data/output/events/motor_policy"),
        Path("/opt/airflow/data/output/discards/motor_policy"),
        Path("/opt/airflow/logs/reports")
    ]

    deleted_count = 0

    for directory in directories:
        if directory.exists():
            for file_path in directory.rglob("*"):
                if file_path.is_file():
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_date:
                        try:
                            file_path.unlink()
                            deleted_count += 1
                        except Exception as e:
                            print(f"Failed to delete {file_path}: {e}")

    print(f"✓ Cleaned up {deleted_count} old files (older than 7 days)")
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
    python_callable=load_metadata_from_database,
    provide_context=True,
    dag=dag
)

run_pipeline = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_full_pipeline,
    provide_context=True,
    execution_timeout=timedelta(hours=1),
    dag=dag
)

check_quality = PythonOperator(
    task_id='check_quality',
    python_callable=check_quality_metrics,
    provide_context=True,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)

send_notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
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
(
    validate_input
    >> load_metadata_task
    >> run_pipeline
    >> check_quality
    >> generate_report_task
    >> send_notification_task
    >> cleanup_task
)