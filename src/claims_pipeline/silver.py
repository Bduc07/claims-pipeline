"""Validate bronze records and write silver and quarantine Parquet tables."""

import logging
from datetime import UTC, datetime
from functools import reduce
from operator import or_
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .rules import (
    CLAIM_RULE_NAMES,
    ClaimRuleSettings,
    Rule,
    RuleEngine,
    Severity,
    TransactionRuleSettings,
    mark_claim_failure,
)

logger = logging.getLogger(__name__)
TRANSACTION_AMOUNT_RULE = "invalid_or_missing_amount"
CHARGE_MINIMUM_RULE = "charge_amount_below_minimum"
MISSING_CLAIM_ID_RULE = "missing_claim_id"


class SilverBatchBlocked(RuntimeError):
    """A blocking quality rule rejected this silver batch."""

    def __init__(self, message: str, rows_quarantined: int) -> None:
        super().__init__(message)
        self.rows_quarantined = rows_quarantined


def _check_reference(
    claims: DataFrame, reference: DataFrame, claim_key: str, reason: str
) -> DataFrame:
    keys = (
        reference.select(F.col("Id").alias("_reference_id"))
        .filter(~F.col("_reference_id").rlike(r"^\s*$"))
        .distinct()
    )
    matched = claims.join(keys, F.col(claim_key) == F.col("_reference_id"), how="left")
    return mark_claim_failure(matched, reason, F.col("_reference_id").isNull()).drop(
        "_reference_id"
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
    ).withColumn(f"_dq_{MISSING_CLAIM_ID_RULE}", F.lit(True))
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
            F.when(F.col("STATUS1") == "UNKNOWN", F.lit("invalid_claim_status")),
        )
    )
    checked = mark_claim_failure(checked, "invalid_service_date", invalid_date)
    if patients is not None:
        checked = _check_reference(checked, patients, "PATIENTID", "unknown_patient")
    if providers is not None:
        checked = _check_reference(checked, providers, "PROVIDERID", "unknown_provider")
    if coverage is not None:
        from .eligibility import flag_uncovered_claims

        checked = flag_uncovered_claims(checked, coverage, ~F.col("_dq_invalid_service_date"))
    return checked.unionByName(rejected, allowMissingColumns=True)


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


def _rule_failed(checked: DataFrame, rule: str) -> Column:
    flag = f"_dq_{rule}"
    if flag in checked.columns:
        return F.col(flag)
    return F.col("quarantine_reason") == rule


def write_silver(
    table: str,
    checked: DataFrame,
    silver_path: str | Path,
    *,
    severity: Severity = "warn",
    report_rule: str | None = None,
    warning_rules: tuple[str, ...] = (),
    configured_rules: tuple[tuple[str, Severity], ...] = (),
    run_id: str | None = None,
) -> tuple[int, int]:
    masked = checked.drop(*(f"_dq_{rule}" for rule in CLAIM_RULE_NAMES)).withColumn(
        "PATIENTID", F.sha2(F.col("PATIENTID"), 256)
    )
    valid = masked.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    invalid = masked.filter(F.col("quarantine_reason").isNotNull())
    reports = [*configured_rules, *((rule, "warn") for rule in warning_rules)]
    if report_rule is not None:
        reports.insert(0, (report_rule, severity))
    counts = checked.agg(
        F.count("*").alias("rows_checked"),
        F.count_if(F.col("quarantine_reason").isNull()).alias("accepted"),
        *(F.count_if(_rule_failed(checked, rule)).alias(rule) for rule, _ in reports),
    ).first()
    accepted = counts.accepted
    rows_checked = counts.rows_checked
    rejected = rows_checked - accepted
    invalid.write.mode("overwrite").parquet(f"{silver_path}/quarantine/{table}")
    blocked = any(level == "block" and counts[rule] > 0 for rule, level in reports)
    if report_rule is None and not configured_rules:
        blocked = blocked or (severity == "block" and rejected > 0)
    if reports:
        entries = []
        for rule, rule_severity in reports:
            failures = counts[rule]
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
        raise SilverBatchBlocked(message, rejected)
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
    started_at = datetime.now(UTC)
    started_clock = perf_counter()
    claim_rules = tuple(
        (rule, severity)
        for rule, severity in claim_settings.rules()
        if rule == MISSING_CLAIM_ID_RULE or f"_dq_{rule}" in claims.columns
    )
    outputs = (
        ("claims", claims, claim_rules),
        (
            "transactions",
            transactions,
            ((TRANSACTION_AMOUNT_RULE, transaction_settings.severity), (CHARGE_MINIMUM_RULE, "warn")),
        ),
    )
    rows_in = None
    quarantined = None
    results: dict[str, tuple[int, int]] = {}
    status = "failed"
    error_type = None
    logger.info("Starting silver run %s", run_id)
    try:
        for table, rows, rules in outputs:
            blockers = [_rule_failed(rows, rule) for rule, level in rules if level == "block"]
            if blockers and rows.filter(reduce(or_, blockers)).limit(1).count():
                # Save quarantine and the rule result before the writer raises.
                rows_in = claims.count() + transactions.count()
                write_silver(table, rows, silver_path, configured_rules=rules, run_id=run_id)

        for table, rows, rules in outputs:
            results[table] = write_silver(
                table, rows, silver_path, configured_rules=rules, run_id=run_id
            )
        quarantined = sum(rejected for _, rejected in results.values())
        rows_in = sum(accepted + rejected for accepted, rejected in results.values())
        status = "succeeded"
        return results
    except SilverBatchBlocked as error:
        status = "blocked"
        error_type = type(error).__name__
        quarantined = error.rows_quarantined
        raise
    except Exception as error:
        error_type = type(error).__name__
        logger.error("Silver run %s failed (%s)", run_id, error_type)
        raise
    finally:
        published = sum(accepted for accepted, _ in results.values())
        duration = perf_counter() - started_clock
        try:
            summary = claims.sparkSession.range(1).select(
                F.lit(run_id).alias("run_id"),
                F.lit("silver").alias("stage"),
                F.lit(started_at).alias("started_at"),
                F.lit(duration).alias("duration_seconds"),
                F.lit(status).alias("status"),
                F.lit(rows_in).cast("long").alias("rows_in"),
                F.lit(published).cast("long").alias("rows_published"),
                F.lit(quarantined).cast("long").alias("rows_quarantined"),
                F.lit(error_type).cast("string").alias("error_type"),
            )
            summary.write.mode("append").parquet(f"{silver_path}/pipeline_runs")
        except Exception:
            if status == "succeeded":
                raise
            logger.exception("Could not record silver run %s", run_id)
        logger.info("Silver run %s ended with status %s after %.2fs", run_id, status, duration)
