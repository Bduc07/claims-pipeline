# Claims Pipeline

A data pipeline that turns synthetic Synthea claims into a weekly payments
report. PySpark runs in Google Colab, and Snowflake runs the reporting SQL.

## How to run

1. Open `claims_pipeline.ipynb` in Colab and run the cells in order.
   Upload `landing.zip`, then upload `src/silver.py` when prompted.
   The notebook writes bronze and silver Parquet files.
2. Run the silver checks in Colab. Expect three passing checks for the supplied batch.
3. Run the database and schema setup at the top of `sql/gold_pipeline.sql`.
4. Upload the two silver CSVs separately into `CLAIMS_PIPELINE.STAGING`
   as `SILVER_CLAIMS` and `SILVER_TRANSACTIONS`.
5. Run the remaining SQL to merge the gold tables and view weekly payments
   by claim batch.

## Verified results

- Bronze processed 107 input files.
- Silver removed 541 duplicate claims and quarantined two invalid records.
- Gold contains 9,227 claims and 89,459 transactions linked to those claims.

## Automated tests

GitHub Actions runs `python -m pytest -q` on pushes and pull requests using
Python 3.12, Java 17, and small synthetic Spark fixtures. With Python and Java
installed, run the same tests locally:

```sh
python -m pip install "pyspark==4.0.1" "pytest>=8,<10"
python -m pytest -q
```
