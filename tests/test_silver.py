import hashlib
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameWriter

from src.claims_pipeline.rules import TransactionRuleSettings
from src.claims_pipeline.silver import (
    TRANSACTION_AMOUNT_RULE,
    SilverBatchBlocked,
    clean_claims,
    clean_transactions,
    write_silver,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("claims-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_silver_claims(spark, tmp_path):
    claims = spark.createDataFrame(
        [
            ("claim-1", "patient-1", "CLOSED", "claims_week.csv"),
            ("claim-1", "patient-1", "UNKNOWN", "claims_week_resend.csv"),
            ("claim-2", "patient-1", "UNKNOWN", "claims_week.csv"),
        ],
        "Id string, PATIENTID string, STATUS1 string, source_file string",
    )
    assert write_silver("claims", clean_claims(claims), tmp_path) == (1, 1)
    [valid] = spark.read.parquet(str(tmp_path / "claims")).collect()
    [invalid] = spark.read.parquet(str(tmp_path / "quarantine" / "claims")).collect()
    assert valid.Id == "claim-1"
    assert valid.source_file == "claims_week.csv"
    assert valid.PATIENTID == hashlib.sha256(b"patient-1").hexdigest()
    assert invalid.PATIENTID == valid.PATIENTID
    assert (invalid.Id, invalid.quarantine_reason) == ("claim-2", "invalid_claim_status")


def test_silver_transactions(spark, tmp_path):
    transactions = spark.createDataFrame(
        [
            ("charge", "patient-1", "CHARGE", "12.34"),
            ("payment", "patient-1", "PAYMENT", None),
            ("transfer", "patient-1", "TRANSFEROUT", ""),
            ("missing", "patient-1", "TRANSFERIN", None),
            ("malformed", "patient-1", "CHARGE", "INVALID"),
        ],
        "ID string, PATIENTID string, TYPE string, AMOUNT string",
    )
    rules_path = Path(__file__).resolve().parents[1] / "src/claims_pipeline/transaction_rules.yaml"
    settings = TransactionRuleSettings.from_yaml(rules_path)
    checked = clean_transactions(transactions, settings)
    assert write_silver(
        "transactions",
        checked,
        tmp_path,
        severity=settings.severity,
        report_rule=TRANSACTION_AMOUNT_RULE,
    ) == (3, 2)
    valid = spark.read.parquet(str(tmp_path / "transactions")).collect()
    invalid = spark.read.parquet(str(tmp_path / "quarantine" / "transactions")).collect()
    assert {row.ID: row.amount_decimal for row in valid} == {
        "charge": Decimal("12.34"),
        "payment": None,
        "transfer": None,
    }
    assert {row.ID: row.quarantine_reason for row in invalid} == {
        "missing": "invalid_or_missing_amount",
        "malformed": "invalid_or_missing_amount",
    }
    assert {row.PATIENTID for row in valid + invalid} == {
        hashlib.sha256(b"patient-1").hexdigest()
    }
    [result] = spark.read.parquet(str(tmp_path / "dq_results")).collect()
    assert (result.rule_name, result.severity) == (TRANSACTION_AMOUNT_RULE, "warn")
    assert (result.rows_checked, result.rows_failed, result.result) == (5, 2, "warned")


def test_transaction_amount_requirement_follows_yaml(spark, tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text("amount_required_types:\n  - PAYMENT\n", encoding="utf-8")
    settings = TransactionRuleSettings.from_yaml(path)
    rows = spark.sql("""
        SELECT * FROM VALUES ('CHARGE', NULL), ('PAYMENT', NULL)
        AS transactions (TYPE, AMOUNT)
    """)
    reasons = {
        row.TYPE: row.quarantine_reason
        for row in clean_transactions(rows, settings).select("TYPE", "quarantine_reason").collect()
    }
    assert reasons == {"CHARGE": None, "PAYMENT": "invalid_or_missing_amount"}


def test_empty_required_types_config_is_invalid(tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text("amount_required_types: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="nonempty list"):
        TransactionRuleSettings.from_yaml(path)


def test_invalid_severity_is_rejected(tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text("severity: stop\namount_required_types: [CHARGE]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="severity must be warn or block"):
        TransactionRuleSettings.from_yaml(path)


def test_block_writes_quarantine_without_publishing_silver(spark, tmp_path, monkeypatch, caplog):
    path = tmp_path / "rules.yaml"
    path.write_text("severity: block\namount_required_types: [CHARGE]\n", encoding="utf-8")
    settings = TransactionRuleSettings.from_yaml(path)
    rows = spark.sql("""
        SELECT * FROM VALUES ('patient-1', 'CHARGE', 'INVALID'), ('patient-2', 'CHARGE', '1.00')
        AS transactions (PATIENTID, TYPE, AMOUNT)
    """)
    checked = clean_transactions(rows, settings)
    writes = []
    def capture_write(writer, destination):
        target = Path(destination)
        writes.append(target)
        if target.name == "dq_results":
            [result] = writer._df.collect()
            assert (result.rows_checked, result.rows_failed, result.result) == (2, 1, "blocked")

    monkeypatch.setattr(DataFrameWriter, "parquet", capture_write)

    with pytest.raises(SilverBatchBlocked, match="1 row"):
        write_silver(
            "transactions",
            checked,
            tmp_path,
            severity=settings.severity,
            report_rule=TRANSACTION_AMOUNT_RULE,
        )

    assert writes == [tmp_path / "quarantine" / "transactions", tmp_path / "dq_results"]
    assert "Silver batch blocked for transactions" in caplog.text
