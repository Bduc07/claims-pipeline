"""Export narrowly scoped Athena tables and a Hadoop Streaming input."""

from pyspark.sql import functions as F

from src.claims_pipeline.config import PipelineConfig
from src.claims_pipeline.pipeline import create_spark


def main() -> None:
    output = PipelineConfig.from_env().output
    gold = output / "gold"
    demo = output / "cloud_demo"
    spark = create_spark()
    try:
        claims = spark.read.parquet(str(gold / "claims")).selectExpr(
            "CLAIM_ID AS claim_id", "BATCH_ID AS batch_week"
        )
        transactions = spark.read.parquet(str(gold / "transactions")).selectExpr(
            "CLAIM_ID AS claim_id", "TRANSACTION_TYPE AS transaction_type",
            "PAYMENT_AMOUNT AS payment_amount",
        )
        claims.write.mode("overwrite").parquet(str(demo / "athena" / "claims"))
        transactions.write.mode("overwrite").parquet(str(demo / "athena" / "transactions"))
        payments = (
            claims.join(transactions.filter(F.col("transaction_type") == "PAYMENT"), "claim_id", "left")
            .select("batch_week", F.coalesce("payment_amount", F.lit(0)).alias("payment_amount"))
        )
        payments.write.mode("overwrite").option("sep", "\t").csv(str(demo / "mapreduce_input"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
