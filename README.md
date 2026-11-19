# Claims Pipeline

A data pipeline that turns synthetic Synthea claims into a weekly payments
report. PySpark runs in Google Colab, and Snowflake runs the reporting SQL.

## How to run

1. Open `claims_pipeline.ipynb` in Colab and run the cells in order.
   Upload `landing.zip`, then `src/claims_pipeline/bronze.py` when prompted.
   At Step 4, upload `silver.py`, `rules.py`, and `transaction_rules.yaml` from
   `src/claims_pipeline/` when prompted.
   The notebook writes bronze and silver Parquet files.
2. Run the silver checks in Colab. Expect three passing checks for the supplied batch.
3. Run the database and schema setup at the top of `sql/gold_pipeline.sql`.
4. Upload the two silver CSVs separately into `CLAIMS_PIPELINE.STAGING`
   as `SILVER_CLAIMS` and `SILVER_TRANSACTIONS`.
5. Run the remaining SQL to merge the gold tables and view weekly payments
   by claim batch.

The transaction amount rule defaults to `severity: warn`. Invalid rows go
to quarantine, and the remaining rows are published. With `severity: block`,
the notebook writes transaction quarantine and stops before publishing new
silver outputs if the rule fails.
The `charge_minimum` setting in `transaction_rules.yaml` defaults to
`0.00`. Charges below it go to quarantine. Payment and transfer amounts
are outside this range check.

Claim rules default to `warn`. You can set `block` independently for
missing IDs, invalid service dates, unknown patients, unknown providers,
and missing coverage in `claim_rules.yaml`. To load overrides in Colab,
put the edited file in `/content/src/claims_pipeline/claim_rules.yaml`
before Step 4. When an enabled rule blocks, silver saves the claim
quarantine and rule results before stopping both publications.

Step 5 shows the latest result for each rule from the append-only
`/content/silver/dq_results` Parquet table. After a blocked run, you can
still run that result cell by itself. The table lasts for the current
Colab runtime.
The amount and charge minimum rules each get a summary row, even with zero
failures. Both use the same batch run ID. The charge minimum is a warning
rule regardless of the configured severity of the amount rule.

Silver appends one run record to `/content/silver/pipeline_runs`. Use its
`run_id` to look up matching `dq_results`. The record includes the stage,
start time, duration, status, and counts. `rows_in` counts the checked
claims after deduplication plus the checked transactions. A blocked run
publishes zero rows. Counts can be unavailable if an unexpected error
interrupts processing.

To apply the optional eligibility rule in a Spark job, pass the Synthea
`payer_transitions.csv` DataFrame as `coverage` to `clean_claims`.
The rule matches patient and primary payer on the service date. Both dates
are inclusive, a blank coverage end is open-ended, and claims without a
primary payer skip this check.
Silver quarantines missing or invalid service dates even when the optional
coverage check is disabled.

Pass the Synthea `patients.csv` and `providers.csv` DataFrames as
`patients` and `providers` to `clean_claims` to enable reference checks.
The checks compare the patient and provider IDs on each claim to the
reference tables before patient IDs
are hashed. Claims with missing or unrecognized patient or provider
references go to quarantine when the corresponding check is enabled.
Reference and coverage checks write audit rows only when their input
tables are supplied. A claim can count toward more than one failed
rule in the audit. Its quarantine row keeps the first failure reason.

## Verified results

- Bronze processed 107 input files.
- Silver removed 541 duplicate claims and quarantined two invalid records.
- Gold contains 9,227 claims and 89,459 transactions linked to those claims.

## Automated tests

GitHub Actions runs `python -m pytest -q` on pushes and pull requests using
Python 3.12, Java 17, and small synthetic Spark fixtures. With Python and Java
installed, run the same tests locally:

```sh
python -m pip install "pyspark==4.0.1" "pytest>=8,<10" "PyYAML>=6,<7"
python -m pytest -q
```

## Production support

Bronze appends file counts, timing, and status to `bronze/pipeline_runs`.
Supply `expected_files` to `run_bronze` to reject a partial delivery
before writing new bronze files. Silver records row counts and quality
outcomes, and Snowflake provides query history for gold.

See [the runbook](RUNBOOK.md) for recovery steps and [the incident notes](docs/incidents/)
for the schema change, late file, and corrupted batch drills.

## Security

Silver requires a keyed masking secret. Colab prompts for it in Step 4.
See [SECURITY.md](SECURITY.md) for field handling, key setup, and the
Snowflake loader and reporter grants.

## Run with Airflow or one command

Docker Compose runs a local Airflow service and Spark. The Airflow DAG
runs bronze, silver, and local gold in order, retries each failed task
once, and saves a JSON alert under `data/output/alerts` if the run fails.
Set `CLAIMS_PIPELINE_ALERT_WEBHOOK` to enable optional Slack-compatible
notifications. The UI listens on localhost.

1. Put the batch CSVs in `data/landing/`, or set
   `CLAIMS_PIPELINE_HOST_DATA` to an existing folder with a `landing/`
   subfolder. Optional inputs are `reference/patients.csv`,
   `reference/providers.csv`, and `reference/payer_transitions.csv`.
   Add `expected_files.txt` to require specific filenames before ingestion.
2. Set `CLAIMS_PIPELINE_HMAC_KEY` using the instructions in SECURITY.md.
   On Linux, set `AIRFLOW_UID` to your user ID with
   `export AIRFLOW_UID=$(id -u)` so the container can write to the data folder.
3. Run `docker compose up --build`. Open `http://localhost:8080`
   and trigger the `claims_pipeline` DAG. The development login is
   `admin`, with the generated password stored in the Airflow container
   at `/opt/airflow/simple_auth_manager_passwords.json.generated`.
4. Check `output/gold/weekly_payments` inside your data folder for Parquet
   results. Run `docker compose down` when finished.

Use one runner at a time for a given data folder.

For a one-command run using the same image and data folder:

```sh
docker compose run --build --rm airflow python -m src.claims_pipeline.pipeline
```

With Python, Java, and the Spark dependencies installed on Linux, the
fallback also works directly:

```sh
python -m src.claims_pipeline.pipeline --data-root data
```

The local runner produces Parquet gold tables and the weekly payments
report. Snowflake loading and the warehouse `MERGE` remain a separate
manual workflow using the silver exports and `sql/gold_pipeline.sql`.

### AWS deployment mapping

The local path has no AWS dependency. For EMR Serverless, land CSVs in
S3, submit a packaged Spark job with an execution role scoped to its
input and output prefixes, and store run logs in S3. Replace local batch
tracking and output publication with S3-backed implementations before
deployment. The Snowflake extension would load S3 results through a
storage integration and stage, then run the existing SQL merges. No AWS
resources were created or EMR jobs executed for this project.
