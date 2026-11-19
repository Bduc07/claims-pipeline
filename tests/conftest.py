from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("claims-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
