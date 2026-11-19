"""Validate bronze records and write silver and quarantine Parquet tables."""

import logging
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .rules import (
    ClaimRuleSettings,
    Rule,
    RuleEngine,
    Severity,
    TransactionRuleSettings,
)

logger = logging.getLogger(__name__)
TRANSACTION_AMOUNT_RULE = "invalid_or_missing_amount"
CHARGE_MINIMUM_RULE = "charge_amount_below_minimum"
MISSING_CLAIM_ID_RULE = "missing_claim_id"


class SilverBatchBlocked(RuntimeError):
    """A blocking quality rule rejected this silver batch."""


def _check_reference(
    claims: DataFrame, reference: DataFrame, claim_key: str, reason: str
) -> DataFrame:
    keys = (
        reference.select(F.col("Id").alias("_reference_id"))
        .filter(~F.col("_reference_id").rlike(r"^\s*$"))
        .distinct()
    )
    return (
        claims.join(
            keys, F.col(claim_key) == F.col("_reference_id"), how="left"
        )
        .withColumn(
            "quarantine_reason",
            F.coalesce(
                F.col("quarantine_reason"),
                F.when(F.col("_reference_id").isNull(), F.lit(reason)),
            ),
        )
        .drop("_reference_id")
    )


def clean_claims(
    claims: DataFrame,
    coverage: DataFrame | None = None,
    *,
    patients: DataFrame | None = None,
    providers: DataFrame | None = None,
) -> DataFrame:
    missing_id = F.coalesce(F.col("Id"), F.lit("")).rlike(r"^\s*$")
    rejected = claims.filter(missing_id).withColumn(
        "quarantine_reason", F.lit(MISSING_CLAIM_ID_RULE)
    )
    order = Window.partitionBy("Id").orderBy(
        F.col("source_file").endswith("_resend.csv").cast("int"),
        F.col("source_file"),
    )
    date_shape = F.col("SERVICEDATE").cast("string").rlike(r"^\d{4}-\d{2}-\d{2}(?:[T ].+)?$")
    invalid_date = ~date_shape | F.expr("try_cast(SERVICEDATE AS TIMESTAMP)").isNull()
    checked = (
        claims.filter(~missing_id).withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn(
            "quarantine_reason",
            F.when(F.col("STATUS1") == "UNKNOWN", F.lit("invalid_claim_status"))
            .when(invalid_date, F.lit("invalid_service_date")),
        )
    )
    if patients is not None:
        checked = _check_reference(checked, patients, "PATIENTID", "unknown_patient")
    if providers is not None:
        checked = _check_reference(checked, providers, "PROVIDERID", "unknown_provider")
    if coverage is not None:
        from .eligibility import flag_uncovered_claims

        checked = flag_uncovered_claims(checked, coverage)
    return checked.unionByName(rejected)


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
    charge_minimum_rule = Rule(
        name=CHARGE_MINIMUM_RULE,
        fails_when=(F.col("TYPE") == "CHARGE")
        & (F.col("amount_decimal") < F.lit(settings.charge_minimum)),
    )
    return RuleEngine((amount_rule, charge_minimum_rule)).annotate(parsed)


def write_silver(
    table: str,
    checked: DataFrame,
    silver_path: str | Path,
    *,
    severity: Severity = "warn",
    report_rule: str | None = None,
    warning_rules: tuple[str, ...] = (),
    run_id: str | None = None,
) -> tuple[int, int]:
    masked = checked.withColumn("PATIENTID", F.sha2(F.col("PATIENTID"), 256))
    valid = masked.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    invalid = masked.filter(F.col("quarantine_reason").isNotNull())
    counts = {
        row.quarantine_reason: row["count"]
        for row in checked.groupBy("quarantine_reason").count().collect()
    }
    accepted = counts.get(None, 0)
    rows_checked = sum(counts.values())
    rejected = rows_checked - accepted
    invalid.write.mode("overwrite").parquet(f"{silver_path}/quarantine/{table}")
    failed = rejected if report_rule is None else counts.get(report_rule, 0)
    blocked = failed > 0 and severity == "block"
    reports = [(rule, "warn") for rule in warning_rules]
    if report_rule is not None:
        reports.insert(0, (report_rule, severity))
    if reports:
        entries = []
        for rule, rule_severity in reports:
            failures = counts.get(rule, 0)
            result = "passed"
            if failures:
                result = "blocked" if rule_severity == "block" else "warned"
            entries.append(
                F.struct(
                    F.lit(rule).alias("rule_name"),
                    F.lit(rule_severity).alias("severity"),
                    F.lit(failures).cast("long").alias("rows_failed"),
                    F.lit(result).alias("result"),
                )
            )
        summary = checked.sparkSession.range(1).select(
            F.explode(F.array(*entries)).alias("rule_result")
        ).select(
            F.lit(run_id or str(uuid4())).alias("run_id"),
            F.current_timestamp().alias("checked_at"),
            F.lit(table).alias("table_name"),
            "rule_result.rule_name",
            "rule_result.severity",
            F.lit(rows_checked).cast("long").alias("rows_checked"),
            "rule_result.rows_failed",
            "rule_result.result",
        )
        summary.write.mode("append").parquet(f"{silver_path}/dq_results")
    if blocked:
        message = f"Silver batch blocked for {table}: {rejected} row(s) quarantined"
        logger.error(message)
        raise SilverBatchBlocked(message)
    if rejected:
        logger.warning("Continuing silver %s with %d quarantined row(s)", table, rejected)
    valid.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    return accepted, rejected


def write_silver_batch(
    claims: DataFrame,
    transactions: DataFrame,
    silver_path: str | Path,
    claim_settings: ClaimRuleSettings,
    transaction_settings: TransactionRuleSettings,
) -> dict[str, tuple[int, int]]:
    """Publish both checked tables after applying blocking rules."""
    run_id = str(uuid4())
    outputs = (
        ("claims", claims, claim_settings.missing_id_severity, MISSING_CLAIM_ID_RULE, ()),
        (
            "transactions",
            transactions,
            transaction_settings.severity,
            TRANSACTION_AMOUNT_RULE,
            (CHARGE_MINIMUM_RULE,),
        ),
    )
    for table, rows, severity, rule, warning_rules in outputs:
        if severity == "block" and rows.filter(F.col("quarantine_reason") == rule).limit(1).count():
            # Save quarantine and the rule result before the writer raises.
            write_silver(
                table,
                rows,
                silver_path,
                severity=severity,
                report_rule=rule,
                warning_rules=warning_rules,
                run_id=run_id,
            )

    return {
        table: write_silver(
            table,
            rows,
            silver_path,
            severity=severity,
            report_rule=rule,
            warning_rules=warning_rules,
            run_id=run_id,
        )
        for table, rows, severity, rule, warning_rules in outputs
    }
