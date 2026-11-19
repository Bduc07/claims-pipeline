"""Apply named row-level data quality checks to Spark DataFrames."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Severity = Literal["warn", "block"]


def _severity(value: object) -> Severity:
    if value == "warn":
        return "warn"
    if value == "block":
        return "block"
    raise ValueError("severity must be warn or block.")


@dataclass(frozen=True)
class ClaimRuleSettings:
    missing_id_severity: Severity = "warn"

    @classmethod
    def from_yaml(cls, path: Path) -> "ClaimRuleSettings":
        with path.open(encoding="utf-8") as config:
            document = yaml.safe_load(config)
        if not isinstance(document, dict) or set(document) != {"missing_claim_id"}:
            raise ValueError("Expected a missing_claim_id setting.")
        rule = document["missing_claim_id"]
        if not isinstance(rule, dict) or set(rule) != {"severity"}:
            raise ValueError("missing_claim_id requires a severity setting.")
        return cls(_severity(rule["severity"]))


@dataclass(frozen=True)
class TransactionRuleSettings:
    amount_required_types: tuple[str, ...]
    severity: Severity = "warn"

    @classmethod
    def from_yaml(cls, path: Path) -> "TransactionRuleSettings":
        with path.open(encoding="utf-8") as config:
            document = yaml.safe_load(config)
        if (
            not isinstance(document, dict)
            or "amount_required_types" not in document
            or set(document) - {"amount_required_types", "severity"}
        ):
            raise ValueError("Expected amount_required_types and optional severity settings.")
        types = document["amount_required_types"]
        if (
            not isinstance(types, list)
            or not types
            or any(not isinstance(value, str) or not value.strip() for value in types)
        ):
            raise ValueError("amount_required_types must be a nonempty list of transaction types.")
        return cls(tuple(types), _severity(document.get("severity", "warn")))


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
