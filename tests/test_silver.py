import hashlib
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from src.claims_pipeline.silver import clean_claims, clean_transactions, write_silver


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
    assert write_silver("transactions", clean_transactions(transactions), tmp_path) == (3, 2)
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
