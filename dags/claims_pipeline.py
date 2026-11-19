"""Schedule the local claims pipeline with retry and failure handling."""

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import DAG, Context, task

from src.claims_pipeline.config import PipelineConfig
from src.claims_pipeline.notifications import notify_failure
from src.claims_pipeline.pipeline import create_spark, run_stage


def on_failure(context: Context) -> None:
    run = context["dag_run"]
    notify_failure(
        PipelineConfig.from_env().output,
        run.dag_id,
        run.run_id,
        os.environ.get("CLAIMS_PIPELINE_ALERT_WEBHOOK"),
    )


with DAG(
    dag_id="claims_pipeline",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    on_failure_callback=on_failure,
    tags=["claims"],
) as dag:

    @task
    def process(stage: str) -> None:
        spark = create_spark()
        try:
            run_stage(stage, spark, PipelineConfig.from_env())
        finally:
            spark.stop()

    bronze = process.override(task_id="bronze")("bronze")
    silver = process.override(task_id="silver")("silver")
    gold = process.override(task_id="gold")("gold")
    bronze >> silver >> gold
