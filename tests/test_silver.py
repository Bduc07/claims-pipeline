import csv
import hashlib
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

from src.claims_pipeline.rules import ClaimRuleSettings, TransactionRuleSettings
from src.claims_pipeline.silver import (
    TRANSACTION_AMOUNT_RULE,
    SilverBatchBlocked,
    clean_claims,
    clean_transactions,
    write_silver,
    write_silver_batch,
)


def test_silver_claims(spark, tmp_path):
    claims = spark.createDataFrame(
        [
            ("claim-1", "patient-1", "CLOSED", "claims_week.csv"),
            ("claim-1", "patient-1", "UNKNOWN", "claims_week_resend.csv"),
            ("claim-2", "patient-1", "UNKNOWN", "claims_week.csv"),
        ],
        "Id string, PATIENTID string, STATUS1 string, source_file string",
    ).withColumn("SERVICEDATE", F.lit("2026-01-05"))
    assert write_silver("claims", clean_claims(claims), tmp_path) == (1, 1)
    [valid] = spark.read.parquet(str(tmp_path / "claims")).collect()
    [invalid] = spark.read.parquet(str(tmp_path / "quarantine" / "claims")).collect()
    assert valid.Id == "claim-1"
    assert valid.source_file == "claims_week.csv"
    assert valid.PATIENTID == hashlib.sha256(b"patient-1").hexdigest()
    assert invalid.PATIENTID == valid.PATIENTID
    assert (invalid.Id, invalid.quarantine_reason) == ("claim-2", "invalid_claim_status")


def test_bronze_rows_reconcile_with_silver(spark):
    claims = spark.sql("""
        SELECT *, '2026-01-05' AS SERVICEDATE FROM VALUES
            ('claim-1', 'CLOSED', 'claims_week.csv'),
            ('claim-1', 'CLOSED', 'claims_week_resend.csv'),
            ('claim-2', 'UNKNOWN', 'claims_week.csv')
        AS claims (Id, STATUS1, source_file)
    """)
    transactions = spark.sql("""
        SELECT * FROM VALUES ('CHARGE', '12.34'), ('PAYMENT', NULL), ('CHARGE', 'INVALID')
        AS transactions (TYPE, AMOUNT)
    """)

    checked_claims = clean_claims(claims).collect()
    valid_claims = [row.Id for row in checked_claims if row.quarantine_reason is None]
    rejected_claims = [row.Id for row in checked_claims if row.quarantine_reason is not None]
    resend_count = claims.filter("source_file = 'claims_week_resend.csv'").count()
    assert (valid_claims, rejected_claims, resend_count) == (["claim-1"], ["claim-2"], 1)
    assert all(row.source_file == "claims_week.csv" for row in checked_claims)
    assert claims.count() == len(valid_claims) + len(rejected_claims) + resend_count

    settings = TransactionRuleSettings(("CHARGE", "TRANSFERIN"))
    checked_transactions = clean_transactions(transactions, settings).collect()
    rejected_transactions = sum(row.quarantine_reason is not None for row in checked_transactions)
    valid_transactions = sum(row.quarantine_reason is None for row in checked_transactions)
    assert (valid_transactions, rejected_transactions) == (2, 1)
    assert transactions.count() == valid_transactions + rejected_transactions


def test_quarantine_matches_answer_key(spark, tmp_path):
    answer_key = tmp_path / "answer_key.csv"
    answer_key.write_text(
        "batch_id,source_file,issue,detail\n"
        "2026-01-05,claims_2026-01-05.csv,invalid_status,claim-2\n"
        "2026-01-05,transactions_2026-01-05.csv,invalid_amount,txn-2\n",
        encoding="utf-8",
    )
    reasons = {
        "invalid_status": "invalid_claim_status",
        "invalid_amount": TRANSACTION_AMOUNT_RULE,
    }
    with answer_key.open(encoding="utf-8", newline="") as source:
        expected = sorted(
            (row["source_file"], row["detail"], reasons[row["issue"]])
            for row in csv.DictReader(source)
            if row["issue"] in reasons
        )

    claims = spark.sql("""
        SELECT *, 'claims_2026-01-05.csv' AS source_file, '2026-01-05' AS SERVICEDATE FROM VALUES
            ('claim-1', 'CLOSED'), ('claim-2', 'UNKNOWN')
        AS claims (Id, STATUS1)
    """)
    transactions = spark.sql("""
        SELECT *, 'transactions_2026-01-05.csv' AS source_file FROM VALUES
            ('txn-1', 'CHARGE', '12.34'), ('txn-2', 'CHARGE', 'INVALID')
        AS transactions (ID, TYPE, AMOUNT)
    """)
    settings = TransactionRuleSettings(("CHARGE", "TRANSFERIN"))
    claim_results = clean_claims(claims).selectExpr(
        "source_file", "Id AS record_id", "quarantine_reason"
    )
    transaction_results = clean_transactions(transactions, settings).selectExpr(
        "source_file", "ID AS record_id", "quarantine_reason"
    )
    quarantined = claim_results.unionByName(transaction_results).filter(
        "quarantine_reason IS NOT NULL"
    )
    assert sorted(tuple(row) for row in quarantined.collect()) == expected


def test_missing_claim_ids_are_preserved_before_deduplication(spark):
    claims = spark.sql("""
        SELECT *, 'claims_week.csv' AS source_file, '2026-01-05' AS SERVICEDATE FROM VALUES
            ('claim-1', 'CLOSED'),
            (NULL, 'CLOSED'),
            (NULL, 'UNKNOWN'),
            ('', 'CLOSED'),
            ('   ', 'CLOSED')
        AS claims (Id, STATUS1)
    """)
    checked = clean_claims(claims)
    [valid] = checked.filter("quarantine_reason IS NULL").select("Id").collect()
    assert valid.Id == "claim-1"
    rejected = checked.filter("quarantine_reason IS NOT NULL").collect()
    assert len(rejected) == 4
    assert {row.quarantine_reason for row in rejected} == {"missing_claim_id"}
    assert sum(row.Id is None for row in rejected) == 2


@pytest.mark.parametrize("severity", ["warn", "block"])
def test_missing_claim_id_severity_controls_publication(spark, tmp_path, monkeypatch, severity):
    config = tmp_path / "claim_rules.yaml"
    config.write_text(f"missing_claim_id:\n  severity: {severity}\n", encoding="utf-8")
    claims = spark.sql("""
        SELECT *, 'claims_week.csv' AS source_file, 'patient-1' AS PATIENTID,
            '2026-01-05' AS SERVICEDATE
        FROM VALUES
            ('claim-1', 'CLOSED'), (NULL, 'CLOSED'), (NULL, 'CLOSED'),
            ('', 'CLOSED'), ('   ', 'CLOSED')
        AS claims (Id, STATUS1)
    """)
    transactions = spark.sql("""
        SELECT 'patient-1' AS PATIENTID, cast(NULL AS STRING) AS quarantine_reason
    """)
    writes = []
    monkeypatch.setattr(DataFrameWriter, "parquet", lambda self, path: writes.append(Path(path)))
    args = (
        clean_claims(claims),
        transactions,
        tmp_path,
        ClaimRuleSettings.from_yaml(config),
        TransactionRuleSettings(("CHARGE",)),
    )
    if severity == "block":
        with pytest.raises(SilverBatchBlocked, match="4 row"):
            write_silver_batch(*args)
        assert writes == [tmp_path / "quarantine" / "claims", tmp_path / "dq_results"]
    else:
        assert write_silver_batch(*args) == {"claims": (1, 4), "transactions": (1, 0)}
        assert tmp_path / "claims" in writes
        assert tmp_path / "transactions" in writes


def test_claim_patient_and_provider_references(spark):
    claims = spark.sql("""
        SELECT *, 'claims_week.csv' AS source_file, '2026-01-05' AS SERVICEDATE FROM VALUES
            ('known', 'patient-1', 'provider-1', 'CLOSED'),
            ('bad-patient', 'patient-2', 'provider-1', 'CLOSED'),
            ('bad-provider', 'patient-1', 'provider-2', 'CLOSED'),
            ('missing-provider', 'patient-1', NULL, 'CLOSED'),
            ('blank-provider', 'patient-1', '', 'CLOSED'),
            ('bad-status', 'patient-2', 'provider-2', 'UNKNOWN')
        AS claims (Id, PATIENTID, PROVIDERID, STATUS1)
    """)
    patients = spark.sql("SELECT 'patient-1' AS Id")
    providers = spark.sql("""
        SELECT * FROM VALUES ('provider-1'), ('provider-1'), ('') AS providers (Id)
    """)

    checked = clean_claims(claims, patients=patients, providers=providers)
    actual = checked.select("Id", "quarantine_reason").collect()
    assert len(actual) == 6
    assert {row.Id: row.quarantine_reason for row in actual} == {
        "known": None,
        "bad-patient": "unknown_patient",
        "bad-provider": "unknown_provider",
        "missing-provider": "unknown_provider",
        "blank-provider": "unknown_provider",
        "bad-status": "invalid_claim_status",
    }


def test_invalid_service_dates_are_flagged_before_eligibility(spark):
    claims = spark.sql("""
        SELECT *, 'claims_week.csv' AS source_file, 'CLOSED' AS STATUS1,
            'patient-1' AS PATIENTID, 'payer-a' AS PRIMARYPATIENTINSURANCEID
        FROM VALUES
            ('valid', '2026-02-10T10:00:00Z'), ('date-only', '2026-02-10'),
            ('blank', '  '), ('missing', NULL), ('impossible', '2026-02-30'),
            ('partial', '2026-02')
        AS claims (Id, SERVICEDATE)
    """)
    coverage = spark.sql("""
        SELECT 'patient-1' AS PATIENT, 'payer-a' AS PAYER,
            '2026-02-01' AS START_DATE, '2026-02-28' AS END_DATE
    """)
    actual = {
        row.Id: row.quarantine_reason
        for row in clean_claims(claims, coverage).select("Id", "quarantine_reason").collect()
    }
    assert actual == {
        "valid": None,
        "date-only": None,
        "blank": "invalid_service_date",
        "missing": "invalid_service_date",
        "impossible": "invalid_service_date",
        "partial": "invalid_service_date",
    }


def test_claim_eligibility_checks_date_and_primary_payer(spark):
    claims = spark.sql("""
        SELECT *, 'claims_week.csv' AS source_file FROM VALUES
            ('start', 'p1', 'payer-a', '2026-01-01T10:00:00Z', 'CLOSED'),
            ('end', 'p1', 'payer-a', '2026-01-31T10:00:00Z', 'CLOSED'),
            ('late', 'p1', 'payer-a', '2026-02-01T10:00:00Z', 'CLOSED'),
            ('wrong-payer', 'p1', 'payer-b', '2026-01-15T10:00:00Z', 'CLOSED'),
            ('other-patient', 'p9', 'payer-a', '2026-01-15T10:00:00Z', 'CLOSED'),
            ('open-ended', 'p2', 'payer-a', '2026-02-01T10:00:00Z', 'CLOSED'),
            ('bad-end', 'p3', 'payer-a', '2026-02-01T10:00:00Z', 'CLOSED'),
            ('no-insurance', 'p9', '', '2026-01-15T10:00:00Z', 'CLOSED'),
            ('bad-status', 'p9', 'payer-a', '2026-01-15T10:00:00Z', 'UNKNOWN')
        AS claims (Id, PATIENTID, PRIMARYPATIENTINSURANCEID, SERVICEDATE, STATUS1)
    """)
    coverage = spark.sql("""
        SELECT * FROM VALUES
            ('p1', 'payer-a', '2026-01-01T00:00:00Z', '2026-01-31T00:00:00Z'),
            ('p1', 'payer-a', '2026-01-01T00:00:00Z', '2026-01-31T00:00:00Z'),
            ('p2', 'payer-a', '2026-01-01T00:00:00Z', NULL),
            ('p3', 'payer-a', '2026-01-01T00:00:00Z', 'INVALID')
        AS coverage (PATIENT, PAYER, START_DATE, END_DATE)
    """)
    actual = clean_claims(claims, coverage).select("Id", "quarantine_reason").collect()
    assert len(actual) == 9
    assert {row.Id: row.quarantine_reason for row in actual} == {
        "start": None,
        "end": None,
        "late": "no_active_coverage",
        "wrong-payer": "no_active_coverage",
        "other-patient": "no_active_coverage",
        "open-ended": None,
        "bad-end": "no_active_coverage",
        "no-insurance": None,
        "bad-status": "invalid_claim_status",
    }


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
