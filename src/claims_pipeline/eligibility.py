"""Check Synthea claims against primary-payer coverage dates."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def flag_uncovered_claims(claims: DataFrame, coverage: DataFrame) -> DataFrame:
    """Label insured claims without active coverage, preserving earlier failures."""
    periods = coverage.select(
        F.col("PATIENT").alias("covered_patient"),
        F.col("PAYER").alias("covered_payer"),
        F.expr("try_cast(substr(START_DATE, 1, 10) AS DATE)").alias("coverage_start"),
        F.expr("try_cast(substr(END_DATE, 1, 10) AS DATE)").alias("coverage_end"),
        (F.length(F.trim(F.coalesce(F.col("END_DATE"), F.lit("")))) == 0)
        .alias("open_ended"),
    )
    insured = F.length(F.trim(F.coalesce(F.col("PRIMARYPATIENTINSURANCEID"), F.lit("")))) > 0
    service_date = F.expr("try_cast(substr(SERVICEDATE, 1, 10) AS DATE)")
    eligible = (
        claims.filter(insured).join(
            periods,
            (F.col("PATIENTID") == F.col("covered_patient"))
            & (F.col("PRIMARYPATIENTINSURANCEID") == F.col("covered_payer"))
            & (service_date >= F.col("coverage_start"))
            & (F.col("open_ended") | (service_date <= F.col("coverage_end"))),
            "left_semi",
        )
        .select("Id")
        .withColumn("_has_coverage", F.lit(True))
    )
    return (
        claims.join(eligible, on="Id", how="left")
        .withColumn(
            "quarantine_reason",
            F.coalesce(
                F.col("quarantine_reason"),
                F.when(insured & F.col("_has_coverage").isNull(), F.lit("no_active_coverage")),
            ),
        )
        .drop("_has_coverage")
    )
