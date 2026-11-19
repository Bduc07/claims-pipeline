"""Apply named row-level data quality checks to Spark DataFrames."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Severity = Literal["warn", "block"]
CLAIM_RULE_NAMES = (
    "missing_claim_id",
    "invalid_service_date",
    "unknown_patient",
    "unknown_provider",
    "no_active_coverage",
)


def _severity(value: object) -> Severity:
    if value == "warn":
        return "warn"
    if value == "block":
        return "block"
    raise ValueError("severity must be warn or block.")


@dataclass(frozen=True)
class ClaimRuleSettings:
    missing_id_severity: Severity = "warn"
    service_date_severity: Severity = "warn"
    patient_severity: Severity = "warn"
    provider_severity: Severity = "warn"
    coverage_severity: Severity = "warn"

    def rules(self) -> tuple[tuple[str, Severity], ...]:
        return (
            ("missing_claim_id", self.missing_id_severity),
            ("invalid_service_date", self.service_date_severity),
            ("unknown_patient", self.patient_severity),
            ("unknown_provider", self.provider_severity),
            ("no_active_coverage", self.coverage_severity),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "ClaimRuleSettings":
        with path.open(encoding="utf-8") as config:
            document = yaml.safe_load(config)
        if not isinstance(document, dict) or set(document) - set(CLAIM_RULE_NAMES):
            raise ValueError("Expected supported claim rule settings.")
        severities = []
        for name in CLAIM_RULE_NAMES:
            rule = document.get(name, {"severity": "warn"})
            if not isinstance(rule, dict) or set(rule) != {"severity"}:
                raise ValueError(f"{name} requires a severity setting.")
            severities.append(_severity(rule["severity"]))
        return cls(*severities)


def mark_claim_failure(claims: DataFrame, rule: str, condition: Column) -> DataFrame:
    """Track each failed rule while retaining the first reason for quarantine."""
    return claims.withColumn(f"_dq_{rule}", condition).withColumn(
        "quarantine_reason",
        F.coalesce(F.col("quarantine_reason"), F.when(F.col(f"_dq_{rule}"), F.lit(rule))),
    )


@dataclass(frozen=True)
class TransactionRuleSettings:
    amount_required_types: tuple[str, ...]
    severity: Severity = "warn"
    charge_minimum: Decimal = Decimal("0.00")

    @classmethod
    def from_yaml(cls, path: Path) -> "TransactionRuleSettings":
        with path.open(encoding="utf-8") as config:
            document = yaml.safe_load(config)
        if (
            not isinstance(document, dict)
            or "amount_required_types" not in document
            or set(document) - {"amount_required_types", "severity", "charge_minimum"}
        ):
            raise ValueError("Expected amount_required_types, with optional severity and charge_minimum.")
        types = document["amount_required_types"]
        if (
            not isinstance(types, list)
            or not types
            or any(not isinstance(value, str) or not value.strip() for value in types)
        ):
            raise ValueError("amount_required_types must be a nonempty list of transaction types.")
        try:
            minimum = Decimal(str(document.get("charge_minimum", "0.00")))
        except InvalidOperation as error:
            raise ValueError("charge_minimum must be a decimal amount.") from error
        if not minimum.is_finite() or (
            not -Decimal("9999999999999999.99") <= minimum <= Decimal("9999999999999999.99")
            or minimum.as_tuple().exponent < -2
        ):
            raise ValueError("charge_minimum must fit DECIMAL(18, 2).")
        return cls(tuple(types), _severity(document.get("severity", "warn")), minimum)


@dataclass(frozen=True)
class Rule:
    name: str
    fails_when: Column


@dataclass(frozen=True)
class RuleEngine:
    rules: tuple[Rule, ...]

    def annotate(self, rows: DataFrame) -> DataFrame:
        """Label each row with the first failing rule, if any."""
        reasons = [F.when(rule.fails_when, F.lit(rule.name)) for rule in self.rules]
        reasons.append(F.lit(None).cast("string"))
        return rows.withColumn("quarantine_reason", F.coalesce(*reasons))
