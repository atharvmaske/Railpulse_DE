
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)


def collect_pipeline_metrics(**context):
    import json
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="railpulse_postgres")
        metrics = []
        for table in ["gold.mart_route_performance", "gold.mart_delay_summary"]:
            try:
                count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
                metrics.append((
                    f"row_count_{table.replace('.','_')}",
                    float(count),
                    json.dumps({"table": table}),
                ))
            except Exception as e:
                logger.warning(f"Could not count {table}: {e}")

        metrics.append(("pipeline_health_check", 1.0, json.dumps({"status": "ok"})))

        if metrics:
            hook.insert_rows(
                table="monitoring.pipeline_health",
                rows=metrics,
                target_fields=["metric_name", "metric_value", "labels"],
            )
            logger.info(f"Wrote {len(metrics)} metrics to monitoring.pipeline_health")
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")


with DAG(
    dag_id="railpulse_monitoring",
    description="Collects pipeline health metrics every 5 minutes",
    default_args={"owner": "railpulse", "retries": 1, "retry_delay": timedelta(minutes=1)},
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["railpulse", "monitoring"],
) as dag:

    collect_metrics = PythonOperator(
        task_id="collect_pipeline_metrics",
        python_callable=collect_pipeline_metrics,
    )