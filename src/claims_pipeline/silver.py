"""Validate bronze records and write silver and quarantine Parquet tables."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_claims(claims):
    order = Window.partitionBy("Id").orderBy(
        F.col("source_file").endswith("_resend.csv").cast("int"),
        F.col("source_file"),
    )
    return (
        claims.withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn(
            "quarantine_reason",
            F.when(F.col("STATUS1") == "UNKNOWN", F.lit("invalid_claim_status")),
        )
    )


def clean_transactions(transactions):
    amount_required = F.col("TYPE").isin("CHARGE", "TRANSFERIN")
    amount_present = F.length(F.trim(F.coalesce(F.col("AMOUNT"), F.lit("")))) > 0
    return (
        transactions.withColumn("amount_decimal", F.expr("try_cast(AMOUNT AS DECIMAL(18, 2))"))
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("amount_decimal").isNull() & (amount_required | amount_present),
                F.lit("invalid_or_missing_amount"),
            ),
        )
    )


def write_silver(table, checked, silver_path):
    masked = checked.withColumn("PATIENTID", F.sha2(F.col("PATIENTID"), 256))
    valid = masked.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    invalid = masked.filter(F.col("quarantine_reason").isNotNull())
    valid.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    invalid.write.mode("overwrite").parquet(f"{silver_path}/quarantine/{table}")
    return valid.count(), invalid.count()
