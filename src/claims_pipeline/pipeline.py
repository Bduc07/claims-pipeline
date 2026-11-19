"""Run all local stages from a shell or invoke them separately in Airflow."""

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .bronze import read_bronze, run_bronze
from .config import PipelineConfig
from .gold import run_gold
from .rules import ClaimRuleSettings, TransactionRuleSettings
from .silver import clean_claims, clean_transactions, write_silver_batch


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("claims-pipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_stage(stage: str, spark: SparkSession, config: PipelineConfig) -> None:
    bronze = config.output / "bronze"
    silver = config.output / "silver"
    if stage == "bronze":
        run_bronze(spark, config.landing, bronze, expected_files=config.expected_files())
    elif stage == "silver":
        def reference(name: str) -> DataFrame | None:
            path = config.root / "reference" / name
            return spark.read.csv(str(path), header=True) if path.is_file() else None

        claims = clean_claims(
            read_bronze(spark, bronze, "claims_*"),
            coverage=reference("payer_transitions.csv"),
            patients=reference("patients.csv"),
            providers=reference("providers.csv"),
        )
        settings = TransactionRuleSettings.from_yaml(config.rule_file("transaction_rules.yaml"))
        transactions = clean_transactions(read_bronze(spark, bronze, "transactions_*"), settings)
        claim_settings = ClaimRuleSettings.from_yaml(config.rule_file("claim_rules.yaml"))
        write_silver_batch(claims, transactions, silver, claim_settings, settings)
    elif stage == "gold":
        run_gold(spark, silver, config.output / "gold")
    else:
        raise ValueError(f"Unknown pipeline stage: {stage}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=PipelineConfig.from_env().root)
    parser.add_argument("--stage", choices=("all", "bronze", "silver", "gold"), default="all")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    spark = create_spark()
    try:
        stages = ("bronze", "silver", "gold") if args.stage == "all" else (args.stage,)
        for stage in stages:
            run_stage(stage, spark, PipelineConfig(args.data_root.resolve()))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
