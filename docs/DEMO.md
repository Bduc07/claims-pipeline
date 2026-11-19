# Demo the claims platform

## Three-minute walkthrough

1. **Show the architecture.** Open the README. Explain that Spark processes
   the Synthea files and Airflow runs bronze, silver, and local gold in
   order. Point to `dq_results` and `pipeline_runs` as the records you use
   to troubleshoot a delivery.
2. **Show a successful run.** Open the [verified GitHub Actions run](https://github.com/Bduc07/claims-pipeline/actions/runs/44560630335).
   In the `airflow` job, open **Run the Airflow DAG and CLI fallback**.
   Show the successful DAG and the check that the CLI rerun skipped both
   bronze files. The fixture has one linked payment worth **12.50**.
3. **Show a failure and recovery.** Open the [corrupted batch drill](incidents/03-corrupted-batch.md)
   and the [runbook](../RUNBOOK.md). Explain how the bad amount stopped
   publication, how its run ID led to the failed rule, and how a corrected
   retry succeeded.
4. **Show the Snowflake report.** In Snowsight, run the final `SELECT` in
   [gold_pipeline.sql](../sql/gold_pipeline.sql). Pick one result and
   explain that `TOTAL_PAID` is grouped by the claim's batch week. The
   original Snowflake run used the manually uploaded silver CSVs.

## Run the local demo from scratch

Follow the README to make the batch data available to Docker Compose and
set the masking key. Run the CLI once:

```sh
docker compose run --build --rm airflow python -m src.claims_pipeline.pipeline
```

To view the results, start a Spark shell in the same image:

```sh
docker compose run --rm airflow bash -c 'exec pyspark'
```

Then run:

```python
spark.read.parquet("/opt/airflow/data/output/gold/weekly_payments").orderBy(
    "CLAIM_BATCH_WEEK", ascending=False
).show(10, truncate=False)
```

Use the saved CI run and the existing Snowflake report when demo time is
tight. Both can be shown without re-uploading anything to Colab.

## What to say about deployment

> I ran the original full synthetic batch in Colab and Snowflake. I also
> verified the Airflow DAG and command-line runner in Docker on GitHub
> Actions with a small fixture. The Airflow pipeline writes local Parquet.
> The Snowflake upload is manual. S3 and EMR Serverless are a documented
> deployment plan.
