from pathlib import Path

from pyspark.sql import SparkSession

from src.claims_pipeline.bronze import run_bronze


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
