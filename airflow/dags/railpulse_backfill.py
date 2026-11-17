
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging

logger = logging.getLogger(__name__)


def replay_bronze_to_kafka(ds: str, **context):
    import boto3
    import os

    logger.info(f"Starting Bronze replay for date={ds}")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://s3.amazonaws.com"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-central-1"),
    )

    bucket = os.getenv("S3_BUCKET_BRONZE", "railpulse-bronze")
    prefix = f"events/event_date={ds}/"

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = response.get("Contents", [])

        logger.info(f"Found {len(objects)} objects in Bronze for {ds}")

        for obj in objects:
            logger.info(f"Would replay: {obj['Key']}")

        context["ti"].xcom_push(key="replayed_count", value=len(objects))

    except Exception as e:
        logger.error(f"Replay failed for {ds}: {e}")
        raise


with DAG(
    dag_id="railpulse_backfill",
    description="Manual backfill — replays Bronze events to Kafka",
    default_args={
        "owner": "railpulse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["railpulse", "backfill", "replay"],
) as dag:

    start = EmptyOperator(task_id="start")

    replay = PythonOperator(
        task_id="replay_bronze_to_kafka",
        python_callable=replay_bronze_to_kafka,
    )

    end = EmptyOperator(task_id="end")

    start >> replay >> end
