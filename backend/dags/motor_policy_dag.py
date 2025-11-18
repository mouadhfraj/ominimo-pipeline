import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago


sys.path.insert(0, '/opt/airflow/src')
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/backend')


from src.services.pipeline import MetadataPipeline
from src.repo import get_db_context, MetadataRepository, PipelineRunRepository, PipelineLogRepository



METADATA_NAME = Variable.get("METADATA_NAME", "motor_policy_airflow")

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


dag = DAG(
    'motor_policy_ingestion',
    default_args=default_args,
    description='Daily motor insurance policy data ingestion and validation (Database-driven with full logging)',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['insurance', 'motor', 'ingestion', 'database']
)


def get_pipeline_id(**context):
    """Get or create pipeline ID from DAG run configuration"""
    dag_run = context.get('dag_run')


    if dag_run and dag_run.conf:
        pipeline_id = dag_run.conf.get('pipeline_id')
        metadata_name = dag_run.conf.get("metadata_name")
        if pipeline_id:
            print(f"Using provided pipeline_id: {pipeline_id}")
            context['task_instance'].xcom_push(key='pipeline_id', value=pipeline_id)
            return pipeline_id


    pipeline_id = f"airflow_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    print(f"Generated new pipeline_id: {pipeline_id}")
    context['task_instance'].xcom_push(key='pipeline_id', value=pipeline_id)
    return pipeline_id


def create_pipeline_record(**context):
    """Create or update pipeline run record in database"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')

    if not pipeline_id:
        raise ValueError("pipeline_id not found in XCom")

    with get_db_context() as db:

        existing_run = PipelineRunRepository.get_by_id(db, pipeline_id)

        if existing_run:
            print(f"Pipeline record already exists: {pipeline_id}")

            PipelineRunRepository.update_status(db, pipeline_id, "running")
        else:
            print(f"Creating new pipeline record: {pipeline_id}")
            PipelineRunRepository.create(
                db,
                pipeline_id=pipeline_id,
                metadata_name=METADATA_NAME,
                status="running"
            )

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Airflow DAG started for pipeline {pipeline_id}",
            stage="initialization"
        )

    return pipeline_id


def validate_input_files(**context):
    """Validate that input files exist and are readable"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')
    input_path = Path("/opt/airflow/data/input/events/motor_policy")

    with get_db_context() as db:
        try:
            if not input_path.exists():
                error_msg = f"Input directory not found: {input_path}"
                PipelineLogRepository.create(
                    db, pipeline_id, "ERROR", error_msg, stage="validation"
                )
                raise FileNotFoundError(error_msg)

            files = list(input_path.glob("*.json"))

            if not files:
                error_msg = f"No JSON files found in {input_path}"
                PipelineLogRepository.create(
                    db, pipeline_id, "ERROR", error_msg, stage="validation"
                )
                raise FileNotFoundError(error_msg)

            context['task_instance'].xcom_push(key='file_count', value=len(files))

            log_msg = f"✓ Found {len(files)} input files to process"
            print(log_msg)
            for f in files:
                print(f"  - {f.name}")

            PipelineLogRepository.create(
                db, pipeline_id, "INFO",
                log_msg,
                stage="validation",
                details={"file_count": len(files), "files": [f.name for f in files]}
            )

            return len(files)

        except Exception as e:
            PipelineLogRepository.create(
                db, pipeline_id, "ERROR",
                f"Validation failed: {str(e)}",
                stage="validation",
                details={"error": str(e)}
            )
            raise


def load_metadata_from_database(**context):
    """Load and validate metadata configuration from database"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')

    print(f"Loading metadata '{METADATA_NAME}' from database...")

    with get_db_context() as db:
        try:
            metadata_file = MetadataRepository.get_by_name(db, METADATA_NAME)

            if not metadata_file:
                error_msg = f"Metadata '{METADATA_NAME}' not found in database"
                PipelineLogRepository.create(
                    db, pipeline_id, "ERROR", error_msg, stage="metadata_loading"
                )
                raise ValueError(error_msg)

            if not metadata_file.is_active:
                error_msg = f"Metadata '{METADATA_NAME}' is not active"
                PipelineLogRepository.create(
                    db, pipeline_id, "ERROR", error_msg, stage="metadata_loading"
                )
                raise ValueError(error_msg)

            metadata = metadata_file.content


            if "dataflows" not in metadata:
                raise ValueError("Invalid metadata: missing 'dataflows'")

            dataflow = metadata["dataflows"][0]


            context['task_instance'].xcom_push(key='dataflow_name', value=dataflow.get('name'))
            context['task_instance'].xcom_push(key='metadata_name', value=METADATA_NAME)
            context['task_instance'].xcom_push(key='metadata_version', value=metadata_file.version)

            log_msg = f"✓ Loaded metadata from database: {METADATA_NAME}"
            print(log_msg)
            print(f"  Version: {metadata_file.version}")
            print(f"  Description: {metadata_file.description}")
            print(f"  Sources: {len(dataflow.get('sources', []))}")
            print(f"  Transformations: {len(dataflow.get('transformations', []))}")
            print(f"  Sinks: {len(dataflow.get('sinks', []))}")

            PipelineLogRepository.create(
                db, pipeline_id, "INFO",
                log_msg,
                stage="metadata_loading",
                details={
                    "version": metadata_file.version,
                    "sources": len(dataflow.get('sources', [])),
                    "transformations": len(dataflow.get('transformations', [])),
                    "sinks": len(dataflow.get('sinks', []))
                }
            )

            return METADATA_NAME

        except Exception as e:
            PipelineLogRepository.create(
                db, pipeline_id, "ERROR",
                f"Metadata loading failed: {str(e)}",
                stage="metadata_loading",
                details={"error": str(e)}
            )
            raise


def serialize_for_json(obj):
    """Recursively convert datetime objects to ISO format strings"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj


def run_full_pipeline(**context):
    """Execute complete pipeline - ingestion, transformation, and storage"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')
    metadata_name = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_name'
    )
    metadata_version = context['task_instance'].xcom_pull(
        task_ids='load_metadata',
        key='metadata_version'
    )

    print(f"Executing pipeline with metadata from database: {metadata_name} v{metadata_version}")

    with get_db_context() as db:
        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Starting pipeline execution with {metadata_name} v{metadata_version}",
            stage="execution"
        )


    pipeline = MetadataPipeline(metadata_name)

    try:

        stats = pipeline.execute()


        valid_count = 0
        invalid_count = 0


        if 'validation_ok' in pipeline.dataframes:
            valid_count = pipeline.dataframes['validation_ok'].count()
        if 'validation_ko' in pipeline.dataframes:
            invalid_count = pipeline.dataframes['validation_ko'].count()

        total_count = valid_count + invalid_count


        context['task_instance'].xcom_push(key='pipeline_stats', value=stats)
        context['task_instance'].xcom_push(key='valid_count', value=valid_count)
        context['task_instance'].xcom_push(key='invalid_count', value=invalid_count)

        print(f"\n{'='*80}")
        print(f"Pipeline Execution Summary")
        print(f"{'='*80}")
        print(f"Pipeline ID: {pipeline_id}")
        print(f"Metadata: {metadata_name} v{metadata_version}")
        print(f"Status: {stats['status']}")
        print(f"Duration: {stats.get('duration_seconds', 0):.2f} seconds")
        print(f"Valid Records: {valid_count}")
        print(f"Invalid Records: {invalid_count}")
        print(f"{'='*80}\n")


        with get_db_context() as db:

            serialized_stages = serialize_for_json(stats.get("stages", {}))

            PipelineRunRepository.update_metrics(
                db,
                pipeline_id,
                total_records=total_count,
                valid_records=valid_count,
                invalid_records=invalid_count,
                stages=serialized_stages
            )


            for stage_name, stage_data in stats.get("stages", {}).items():
                PipelineLogRepository.create(
                    db, pipeline_id, "INFO",
                    f"Stage {stage_name} completed with status: {stage_data.get('status', 'unknown')}",
                    stage=stage_name,
                    details=serialize_for_json(stage_data)
                )


        if stats['status'] != 'success':
            raise RuntimeError(f"Pipeline failed with status: {stats['status']}")

        return stats

    except Exception as e:
        print(f"✗ Pipeline execution failed: {str(e)}")

        with get_db_context() as db:
            PipelineLogRepository.create(
                db, pipeline_id, "ERROR",
                f"Pipeline execution failed: {str(e)}",
                stage="execution",
                details={"error": str(e), "error_type": type(e).__name__}
            )
        raise
    finally:
        pipeline.stop()


def check_quality_metrics(**context):
    """Check if pipeline met quality standards"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')
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
        error_msg = "No records processed!"
        with get_db_context() as db:
            PipelineLogRepository.create(
                db, pipeline_id, "ERROR", error_msg, stage="quality_check"
            )
        raise ValueError(error_msg)

    valid_percentage = (valid_count / total_count) * 100


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

    with get_db_context() as db:
        if valid_percentage < min_valid_percentage:
            error_msg = f"Quality check failed: {valid_percentage:.2f}% valid records (minimum: {min_valid_percentage}%)"
            print(f"✗ FAILED: {error_msg}")
            print(f"{'='*80}\n")

            PipelineLogRepository.create(
                db, pipeline_id, "ERROR",
                error_msg,
                stage="quality_check",
                details={
                    "valid_percentage": valid_percentage,
                    "required_minimum": min_valid_percentage,
                    "total_records": total_count,
                    "valid_records": valid_count,
                    "invalid_records": invalid_count
                }
            )
            raise ValueError(error_msg)

        print(f"✓ PASSED: Quality standards met")
        print(f"{'='*80}\n")

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Quality check passed: {valid_percentage:.2f}% valid records",
            stage="quality_check",
            details={
                "valid_percentage": valid_percentage,
                "total_records": total_count,
                "valid_records": valid_count,
                "invalid_records": invalid_count
            }
        )

    return {
        "total": total_count,
        "valid": valid_count,
        "invalid": invalid_count,
        "valid_percentage": valid_percentage,
        "quality_check": "PASSED"
    }


def update_pipeline_status_success(**context):
    """Update pipeline status to success in database"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')

    with get_db_context() as db:
        PipelineRunRepository.update_status(
            db, pipeline_id, "success", end_time=datetime.utcnow()
        )

        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            "Pipeline completed successfully",
            stage="completion"
        )

    print(f"✓ Pipeline {pipeline_id} marked as SUCCESS in database")
    return pipeline_id


def update_pipeline_status_failed(**context):
    """Update pipeline status to failed in database"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')


    task_instance = context.get('task_instance')
    error_msg = "Pipeline failed"

    if task_instance:
        try:

            error_msg = str(context.get('exception', 'Unknown error'))
        except:
            pass

    with get_db_context() as db:
        PipelineRunRepository.update_status(
            db, pipeline_id, "failed",
            end_time=datetime.utcnow(),
            error_message=error_msg
        )

        PipelineLogRepository.create(
            db, pipeline_id, "ERROR",
            f"Pipeline failed: {error_msg}",
            stage="failure"
        )

    print(f"✗ Pipeline {pipeline_id} marked as FAILED in database")
    return pipeline_id


def generate_report(**context):
    """Generate pipeline execution report"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')
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
        "pipeline_id": pipeline_id,
        "metadata_name": metadata_name,
        "metadata_version": metadata_version,
        "status": pipeline_stats.get('status'),
        "duration_seconds": pipeline_stats.get('duration_seconds'),
        "quality_metrics": quality_metrics,
        "stages": pipeline_stats.get('stages', {}),
        "source": "airflow"
    }


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

    with get_db_context() as db:
        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Execution report generated and saved to {report_path}",
            stage="reporting",
            details={"report_path": str(report_path)}
        )

    return report


def send_notification(**context):
    """Send notification about pipeline execution"""
    report = context['task_instance'].xcom_pull(task_ids='generate_report')
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')

    status = report['status']
    quality = report['quality_metrics']

    message = f"""
Motor Policy Pipeline Execution Report
{'='*50}

Pipeline ID: {pipeline_id}
Status: {status.upper()}
Execution Date: {report['execution_date']}
Duration: {report['duration_seconds']:.2f} seconds

Metadata:
  Name: {report['metadata_name']}
  Version: {report['metadata_version']}
  Source: Airflow

Data Quality:
  Total Records: {quality['total']}
  Valid Records: {quality['valid']} ({quality['valid_percentage']:.2f}%)
  Invalid Records: {quality['invalid']}
  Quality Check: {quality['quality_check']}
"""

    print(message)

    with get_db_context() as db:
        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            "Notification sent",
            stage="notification",
            details={"status": status, "quality_metrics": quality}
        )

    print("✓ Notification logged")
    return message


def cleanup_old_files(**context):
    """Clean up old processed files (older than 7 days)"""
    pipeline_id = context['task_instance'].xcom_pull(task_ids='get_pipeline_id', key='pipeline_id')
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

    with get_db_context() as db:
        PipelineLogRepository.create(
            db, pipeline_id, "INFO",
            f"Cleaned up {deleted_count} old files",
            stage="cleanup",
            details={"deleted_count": deleted_count}
        )

    return deleted_count



get_pipeline_id_task = PythonOperator(
    task_id='get_pipeline_id',
    python_callable=get_pipeline_id,
    provide_context=True,
    dag=dag
)

create_pipeline_record_task = PythonOperator(
    task_id='create_pipeline_record',
    python_callable=create_pipeline_record,
    provide_context=True,
    dag=dag
)

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

update_success_task = PythonOperator(
    task_id='update_success',
    python_callable=update_pipeline_status_success,
    provide_context=True,
    trigger_rule='all_success',
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

update_failed_task = PythonOperator(
    task_id='update_failed',
    python_callable=update_pipeline_status_failed,
    provide_context=True,
    trigger_rule='one_failed',
    dag=dag
)


(
    get_pipeline_id_task
    >> create_pipeline_record_task
    >> validate_input
    >> load_metadata_task
    >> run_pipeline
    >> check_quality
    >> update_success_task
    >> generate_report_task
    >> send_notification_task
    >> cleanup_task
)


run_pipeline >> update_failed_task
check_quality >> update_failed_task

