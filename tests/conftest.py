from collections.abc import Iterator
from secrets import token_hex

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def masking_key(monkeypatch: pytest.MonkeyPatch) -> bytes:
    secret = token_hex(32)
    monkeypatch.setenv("CLAIMS_PIPELINE_HMAC_KEY", secret)
    return secret.encode("utf-8")


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
