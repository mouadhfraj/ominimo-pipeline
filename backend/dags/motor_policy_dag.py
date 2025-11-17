"""
Airflow DAG for Motor Insurance Policy Pipeline
Orchestrates daily ingestion and processing of motor policy data
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add the source directory to Python path
sys.path.insert(0, '/opt/airflow/src')
sys.path.insert(0, '/opt/airflow')

# Import using absolute imports
from src.services.pipeline import MetadataPipeline


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

    print(f"✓ Found {len(files)} input files to process")
    for f in files:
        print(f"  - {f.name}")

    return len(files)


def load_metadata(**context):
    """Load and validate metadata configuration"""
    metadata_path = "/opt/airflow/metadata/motor_policy_airflow.json"

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    # Validate metadata structure
    if "dataflows" not in metadata:
        raise ValueError("Invalid metadata: missing 'dataflows'")

    dataflow = metadata["dataflows"][0]
    context['task_instance'].xcom_push(key='dataflow_name', value=dataflow.get('name'))
    context['task_instance'].xcom_push(key='metadata_path', value=metadata_path)

    print(f"✓ Loaded metadata for dataflow: {dataflow.get('name')}")
    print(f"  Version: {dataflow.get('version')}")
    print(f"  Sources: {len(dataflow.get('sources', []))}")
    print(f"  Transformations: {len(dataflow.get('transformations', []))}")
    print(f"  Sinks: {len(dataflow.get('sinks', []))}")

    return metadata_path


def run_full_pipeline(**context):
    """Execute complete pipeline - ingestion, transformation, and storage"""
    metadata_path = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_path'
    )

    print(f"Executing pipeline with metadata: {metadata_path}")

    pipeline = MetadataPipeline(metadata_path)

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

        for transform in transformation_stats.get('transformations', []):
            if transform.get('type') == 'validate_fields':
                # These counts are in the input_count of subsequent transformations
                pass

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

    total_count = valid_count + invalid_count

    if total_count == 0:
        raise ValueError("No records processed!")

    valid_percentage = (valid_count / total_count) * 100

    # Load quality rules from metadata
    metadata_path = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_path'
    )

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

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

    execution_date = context['execution_date']
    dag_run_id = context['dag_run'].run_id

    report = {
        "execution_date": execution_date.isoformat(),
        "dag_run_id": dag_run_id,
        "pipeline_id": pipeline_stats.get('pipeline_id', 'unknown'),
        "status": pipeline_stats.get('status'),
        "duration_seconds": pipeline_stats.get('duration_seconds'),
        "quality_metrics": quality_metrics,
        "stages": pipeline_stats.get('stages', {}),
        "metadata_version": pipeline_stats.get('metadata_version')
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

Data Quality:
  Total Records: {quality['total']}
  Valid Records: {quality['valid']} ({quality['valid_percentage']:.2f}%)
  Invalid Records: {quality['invalid']}
  Quality Check: {quality['quality_check']}

Pipeline ID: {report['pipeline_id']}
Metadata Version: {report['metadata_version']}
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
    python_callable=load_metadata,
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