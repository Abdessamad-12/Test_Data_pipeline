"""DAG : SODA -> bronze -> silver -> gold (datalake MinIO). Lancé une fois au démarrage (@once)."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="taxi_pipeline",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["datalake", "taxi"],
)
def taxi_pipeline():
    @task
    def ingest_bronze():
        from common import config
        from ingestion import api_soda
        return api_soda.run(config.START_MONTH, config.END_MONTH)

    @task
    def bronze_to_silver():
        from transformation import bronze_to_silver as job
        job.run()

    @task
    def silver_to_gold():
        from transformation import silver_to_gold as job
        job.run()

    ingest_bronze() >> bronze_to_silver() >> silver_to_gold()


taxi_pipeline()