"""Validate bronze records and write silver and quarantine Parquet tables."""

import logging
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .rules import Rule, RuleEngine, Severity, TransactionRuleSettings

logger = logging.getLogger(__name__)
TRANSACTION_AMOUNT_RULE = "invalid_or_missing_amount"


class SilverBatchBlocked(RuntimeError):
    """A blocking quality rule rejected this silver batch."""


def clean_claims(claims: DataFrame, coverage: DataFrame | None = None) -> DataFrame:
    order = Window.partitionBy("Id").orderBy(
        F.col("source_file").endswith("_resend.csv").cast("int"),
        F.col("source_file"),
    )
    checked = (
        claims.withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn(
            "quarantine_reason",
            F.when(F.col("STATUS1") == "UNKNOWN", F.lit("invalid_claim_status")),
        )
    )
    if coverage is not None:
        from .eligibility import flag_uncovered_claims

        return flag_uncovered_claims(checked, coverage)
    return checked


def clean_transactions(
    transactions: DataFrame, settings: TransactionRuleSettings
) -> DataFrame:
    amount_required = F.col("TYPE").isin(*settings.amount_required_types)
    amount_present = F.length(F.trim(F.coalesce(F.col("AMOUNT"), F.lit("")))) > 0
    parsed = transactions.withColumn(
        "amount_decimal", F.expr("try_cast(AMOUNT AS DECIMAL(18, 2))")
    )
    amount_rule = Rule(
        name=TRANSACTION_AMOUNT_RULE,
        fails_when=F.col("amount_decimal").isNull() & (amount_required | amount_present),
    )
    return RuleEngine((amount_rule,)).annotate(parsed)


def write_silver(
    table: str,
    checked: DataFrame,
    silver_path: str | Path,
    *,
    severity: Severity = "warn",
    report_rule: str | None = None,
) -> tuple[int, int]:
    masked = checked.withColumn("PATIENTID", F.sha2(F.col("PATIENTID"), 256))
    valid = masked.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    invalid = masked.filter(F.col("quarantine_reason").isNotNull())
    rejected = invalid.count()
    invalid.write.mode("overwrite").parquet(f"{silver_path}/quarantine/{table}")
    blocked = rejected > 0 and severity == "block"
    if report_rule is not None:
        failed = invalid.filter(F.col("quarantine_reason") == report_rule).count()
        result = "blocked" if blocked else "warned" if failed else "passed"
        summary = checked.sparkSession.range(1).select(
            F.lit(str(uuid4())).alias("run_id"),
            F.current_timestamp().alias("checked_at"),
            F.lit(table).alias("table_name"),
            F.lit(report_rule).alias("rule_name"),
            F.lit(severity).alias("severity"),
            F.lit(checked.count()).cast("long").alias("rows_checked"),
            F.lit(failed).cast("long").alias("rows_failed"),
            F.lit(result).alias("result"),
        )
        summary.write.mode("append").parquet(f"{silver_path}/dq_results")
    if blocked:
        message = f"Silver batch blocked for {table}: {rejected} row(s) quarantined"
        logger.error(message)
        raise SilverBatchBlocked(message)
    if rejected:
        logger.warning("Continuing silver %s with %d quarantined row(s)", table, rejected)
    valid.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    return valid.count(), rejected
