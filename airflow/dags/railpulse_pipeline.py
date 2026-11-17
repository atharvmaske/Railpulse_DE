from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging

DBT_CMD = "python -c \"from dbt.cli.main import cli; cli()\""

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "railpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_DIR = "/opt/airflow/dbt_transform"


def check_kafka_freshness(**context):
    import os
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")})
        topics = admin.list_topics(timeout=10)
        if "raw-events" not in topics.topics:
            raise ValueError("raw-events topic not found")
        logger.info("Kafka healthy")
        return "kafka_healthy"
    except Exception as e:
        logger.error(f"Kafka check failed: {e}")
        return "kafka_unhealthy"


def load_gtfs_static(**context):
    import requests, boto3, os
    from io import BytesIO
    url = os.getenv("GTFS_STATIC_URL", "https://download.gtfs.de/germany/free/latest.zip")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://s3.amazonaws.com"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        key = f"gtfs_static/{context['ds']}/gtfs_static.zip"
        s3.upload_fileobj(BytesIO(resp.content), os.getenv("S3_BUCKET_BRONZE", "railpulse-bronze"), key)
        logger.info(f"Uploaded {key}")
    except Exception as e:
        logger.warning(f"GTFS Static failed (non-critical): {e}")


def check_slo_breaches(**context):
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="railpulse_postgres")
        rows = hook.get_records(
            "SELECT network_on_time_rate_pct FROM gold.mart_delay_summary "
            "WHERE event_date = CURRENT_DATE LIMIT 1"
        )
        if not rows:
            return "slo_ok"
        rate = rows[0][0]
        logger.info(f"On-time rate: {rate}%")
        return "slo_breached" if rate and rate < 75 else "slo_ok"
    except Exception as e:
        logger.error(f"SLO check failed: {e}")
        return "slo_ok"


def send_slo_alert(**context):
    logger.warning(f"SLO BREACH on {context['ds']} — network on-time rate below 75%")


with DAG(
    dag_id="railpulse_pipeline",
    description="Daily: GTFS Static + dbt Gold + SLO checks",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["railpulse", "pipeline", "gold"],
) as dag:

    start = EmptyOperator(task_id="start")

    kafka_check = BranchPythonOperator(
        task_id="check_kafka_freshness",
        python_callable=check_kafka_freshness,
    )

    kafka_healthy = EmptyOperator(task_id="kafka_healthy")
    kafka_unhealthy = EmptyOperator(task_id="kafka_unhealthy")

    load_static = PythonOperator(
        task_id="load_gtfs_static",
        python_callable=load_gtfs_static,
        trigger_rule="none_failed_min_one_success",
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} run --profiles-dir . --select staging",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} run --profiles-dir . --select intermediate",
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} run --profiles-dir . --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} test --profiles-dir .",
    )
    slo_check = BranchPythonOperator(
        task_id="check_slo_breaches",
        python_callable=check_slo_breaches,
        trigger_rule="all_done",
    )

    slo_ok = EmptyOperator(task_id="slo_ok")

    slo_breached = PythonOperator(
        task_id="slo_breached",
        python_callable=send_slo_alert,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> kafka_check >> [kafka_healthy, kafka_unhealthy]
    kafka_healthy >> load_static
    kafka_unhealthy >> load_static
    load_static >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
    dbt_test >> slo_check >> [slo_ok, slo_breached]
    [slo_ok, slo_breached] >> end