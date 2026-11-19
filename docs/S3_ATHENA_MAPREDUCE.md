# S3, Athena, and Hadoop demo

The demo compares weekly paid amounts across Spark, Athena, and Hadoop
MapReduce using the same cleaned claims and transactions. Use only
synthetic inputs and a fresh output folder for a new delivery.

## Build the outputs and run Hadoop

Prepare the data folder and masking key as described in the [README](../README.md).
Run these from the repository root:

```sh
docker compose run --build --rm airflow python -m src.claims_pipeline.pipeline
docker compose run --rm airflow python /opt/airflow/project/scripts/export_cloud_demo.py
docker compose run --rm airflow bash /opt/airflow/project/scripts/run_mapreduce.sh
```

Hadoop Streaming runs in local mode in this container. The mapper emits
a week and integer cents. Hadoop groups the weeks, and the reducer sums
each group. The output is `output/cloud_demo/mapreduce_output/part-00000`
under the data folder. To repeat the job, pass a fresh output path as the
argument after `run_mapreduce.sh`. The input has a zero amount for claims
without a linked payment, so those weeks appear in the output.

## Publish one synthetic snapshot to S3

1. In the AWS console, create or select a private S3 bucket with Block
   Public Access enabled. Use the same AWS Region for Athena.
2. Optionally upload the original synthetic CSV pair to
   `claims-demo/landing/` to show the incoming files. Athena uses the
   gold exports.
3. Upload the `.parquet` files from
   `output/cloud_demo/athena/claims/` to `claims-demo/athena/claims/`
   and the files from `output/cloud_demo/athena/transactions/` to
   `claims-demo/athena/transactions/`. These exports include only
   fields needed for the weekly report.
4. In Athena, select a workgroup and set its query results location to
   a separate S3 prefix, such as `s3://YOUR_BUCKET/claims-demo/results/`,
   if the workgroup does not already manage results.
5. Open [the Athena SQL](../sql/athena_weekly_payments.sql), replace
   `YOUR_BUCKET`, and run the statements individually. Compare the
   final `total_paid` values with Hadoop's output and Spark's
   `output/gold/weekly_payments` report.

Use a fresh bucket prefix or replace the old snapshot when repeating the
demo. Leaving both sets of Parquet files in a prefix can double-count
payments. The CI fixture expects **12.50** for the week of 2026-01-05.
The live S3 upload and Athena query need an AWS account with access to
the bucket, Athena, and the Glue Data Catalog. Review your plan and credits
in AWS before running the demo.
