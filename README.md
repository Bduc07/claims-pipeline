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

The missing claim ID rule uses `claim_rules.yaml`. It defaults to `warn`
in Colab. To try `block`, put the edited file in
`/content/src/claims_pipeline/claim_rules.yaml` before Step 4. A missing ID
then stops both silver outputs after saving the claim quarantine and result.

Step 5 shows the latest result for each rule from the append-only
`/content/silver/dq_results` Parquet table. After a blocked run, you can
still run that result cell by itself. The table lasts for the current
Colab runtime.

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
