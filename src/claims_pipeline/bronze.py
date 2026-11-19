import logging
from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchTracker:
    output_dir: Path

    def destination(self, source: Path) -> Path:
        return self.output_dir / source.stem

    def is_processed(self, source: Path) -> bool:
        return (self.destination(source) / "_SUCCESS").is_file()


def read_bronze(spark: SparkSession, bronze: Path, pattern: str) -> DataFrame:
    paths = [str(path) for path in sorted(bronze.glob(pattern)) if (path / "_SUCCESS").is_file()]
    if not paths:
        raise FileNotFoundError(f"No completed bronze outputs match {pattern} in {bronze}")
    return spark.read.option("mergeSchema", "true").parquet(*paths)


def run_bronze(
    spark: SparkSession,
    landing: Path,
    bronze: Path,
    *,
    expected_files: Collection[str] = (),
) -> tuple[int, int]:
    run_id = str(uuid4())
    started_at = datetime.now(UTC)
    started_clock = perf_counter()
    tracker = BatchTracker(bronze)
    found = written = skipped = 0
    status = "failed"
    error_type = None
    logger.info("Starting bronze run %s", run_id)
    try:
        sources = sorted(landing.rglob("*.csv"))
        found = len(sources)
        missing = sorted(set(expected_files) - {source.name for source in sources})
        if missing:
            raise FileNotFoundError(f"Missing expected delivery files: {', '.join(missing)}")
        if not sources:
            raise ValueError(f"No CSV files found in {landing}")

        bronze.mkdir(parents=True, exist_ok=True)
        for source in sources:
            if tracker.is_processed(source):
                skipped += 1
                continue

            destination = tracker.destination(source)
            staging = bronze / f".{source.stem}.tmp"
            batch_id = source.stem.split("_")[1]
            rows = (
                spark.read.csv(str(source), header=True, inferSchema=False)
                .withColumn("source_file", F.lit(source.name))
                .withColumn("ingest_time", F.current_timestamp())
                .withColumn("batch_id", F.lit(batch_id))
            )
            rows.write.mode("overwrite").parquet(str(staging))
            staging.rename(destination)
            written += 1

        status = "succeeded"
        return written, skipped
    except Exception as error:
        error_type = type(error).__name__
        logger.error("Bronze run %s failed (%s): %s", run_id, error_type, error)
        raise
    finally:
        duration = perf_counter() - started_clock
        try:
            summary = spark.range(1).select(
                F.lit(run_id).alias("run_id"),
                F.lit("bronze").alias("stage"),
                F.lit(started_at).alias("started_at"),
                F.lit(duration).alias("duration_seconds"),
                F.lit(status).alias("status"),
                F.lit(found).cast("long").alias("files_found"),
                F.lit(written).cast("long").alias("files_written"),
                F.lit(skipped).cast("long").alias("files_skipped"),
                F.lit(error_type).cast("string").alias("error_type"),
            )
            summary.write.mode("append").parquet(str(bronze / "pipeline_runs"))
        except Exception:
            if status == "succeeded":
                raise
            logger.exception("Could not record bronze run %s", run_id)
        logger.info("Bronze run %s ended with status %s after %.2fs", run_id, status, duration)
