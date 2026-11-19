from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.claims_pipeline.bronze import read_bronze, run_bronze


def test_rerun_leaves_bronze_parquet_unchanged(spark: SparkSession, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    source = landing / "claims_2026-01-05.csv"
    source.write_text("Id,STATUS1\nclaim-1,CLOSED\nclaim-2,UNKNOWN\n", encoding="utf-8")
    bronze = tmp_path / "bronze"
    output = bronze / source.stem

    assert run_bronze(spark, landing, bronze) == (1, 0)
    assert (output / "_SUCCESS").is_file()
    parquet_files = sorted(output.glob("*.parquet"))
    original = spark.read.parquet(str(output)).orderBy("Id").collect()
    assert [(row.Id, row.STATUS1) for row in original] == [
        ("claim-1", "CLOSED"),
        ("claim-2", "UNKNOWN"),
    ]
    assert {row.source_file for row in original} == {source.name}
    assert {row.batch_id for row in original} == {"2026-01-05"}
    assert all(row.ingest_time is not None for row in original)

    assert run_bronze(spark, landing, bronze) == (0, 1)
    assert sorted(output.glob("*.parquet")) == parquet_files
    assert spark.read.parquet(str(output)).orderBy("Id").collect() == original

    runs = spark.read.parquet(str(bronze / "pipeline_runs")).orderBy("started_at").collect()
    assert [(row.status, row.files_found, row.files_written, row.files_skipped) for row in runs] == [
        ("succeeded", 1, 1, 0),
        ("succeeded", 1, 0, 1),
    ]
    assert len({row.run_id for row in runs}) == 2
    assert all(row.stage == "bronze" and row.duration_seconds >= 0 for row in runs)


def test_schema_change_drill_preserves_added_column(spark: SparkSession, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    bronze = tmp_path / "bronze"
    (landing / "claims_2026-01-05.csv").write_text("Id\nclaim-1\n", encoding="utf-8")
    (landing / "claims_2026-01-12.csv").write_text(
        "Id,SOURCE_SYSTEM\nclaim-2,SYNTHEA\n", encoding="utf-8"
    )

    assert run_bronze(spark, landing, bronze) == (2, 0)
    rows = read_bronze(spark, bronze, "claims_*").orderBy("Id").collect()
    assert [(row.Id, row.SOURCE_SYSTEM) for row in rows] == [
        ("claim-1", None),
        ("claim-2", "SYNTHEA"),
    ]


def test_late_file_drill_waits_for_expected_delivery(spark: SparkSession, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    bronze = tmp_path / "bronze"
    claims = "claims_2026-01-05.csv"
    transactions = "transactions_2026-01-05.csv"
    (landing / claims).write_text("Id\nclaim-1\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match=transactions):
        run_bronze(spark, landing, bronze, expected_files=(claims, transactions))
    assert not (bronze / Path(claims).stem).exists()

    (landing / transactions).write_text("ID\ntxn-1\n", encoding="utf-8")
    assert run_bronze(spark, landing, bronze, expected_files=(claims, transactions)) == (2, 0)
    runs = spark.read.parquet(str(bronze / "pipeline_runs")).orderBy("started_at").collect()
    assert [(row.status, row.files_found, row.files_written, row.error_type) for row in runs] == [
        ("failed", 1, 0, "FileNotFoundError"),
        ("succeeded", 2, 2, None),
    ]
