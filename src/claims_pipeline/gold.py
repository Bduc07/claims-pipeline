"""Build a local Spark equivalent of the Snowflake gold report."""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def run_gold(spark: SparkSession, silver: Path, gold: Path) -> None:
    claims = spark.read.parquet(str(silver / "claims")).selectExpr(
        "Id AS CLAIM_ID",
        "PATIENTID AS PATIENT_KEY",
        "PROVIDERID AS PROVIDER_ID",
        "try_cast(substr(SERVICEDATE, 1, 10) AS DATE) AS SERVICE_DATE",
        "STATUS1 AS CLAIM_STATUS",
        "batch_id AS BATCH_ID",
    )
    transactions = spark.read.parquet(str(silver / "transactions")).selectExpr(
        "ID AS TRANSACTION_ID",
        "CLAIMID AS CLAIM_ID",
        "TYPE AS TRANSACTION_TYPE",
        "amount_decimal AS AMOUNT",
        "try_cast(PAYMENTS AS DECIMAL(18, 2)) AS PAYMENT_AMOUNT",
        "batch_id AS BATCH_ID",
    ).join(claims.select("CLAIM_ID"), "CLAIM_ID", "left_semi")
    report = (
        claims.alias("claims")
        .join(transactions.alias("transactions"), "CLAIM_ID", "left")
        .groupBy(F.col("claims.BATCH_ID").alias("CLAIM_BATCH_WEEK"))
        .agg(
            F.countDistinct("CLAIM_ID").alias("CLAIM_COUNT"),
            F.sum(
                F.when(
                    F.col("TRANSACTION_TYPE") == "PAYMENT", F.coalesce("PAYMENT_AMOUNT", F.lit(0))
                ).otherwise(F.lit(0))
            ).alias("TOTAL_PAID"),
        )
    )
    for name, table in (
        ("claims", claims), ("transactions", transactions), ("weekly_payments", report)
    ):
        table.write.mode("overwrite").parquet(str(gold / name))
        logger.info("Wrote gold %s to %s", name, gold / name)
