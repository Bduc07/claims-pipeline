from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class BatchTracker:
    output_dir: Path

    def destination(self, source: Path) -> Path:
        return self.output_dir / source.stem

    def is_processed(self, source: Path) -> bool:
        return (self.destination(source) / "_SUCCESS").is_file()


def run_bronze(
    spark: SparkSession,
    landing: Path,
    bronze: Path,
) -> tuple[int, int]:
    sources = sorted(landing.rglob("*.csv"))
    if not sources:
        raise ValueError(f"No CSV files found in {landing}")

    bronze.mkdir(parents=True, exist_ok=True)
    tracker = BatchTracker(bronze)
    written = skipped = 0

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

    return written, skipped