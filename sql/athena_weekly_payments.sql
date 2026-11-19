-- Replace YOUR_BUCKET with your private bucket before running in Athena.
CREATE DATABASE IF NOT EXISTS claims_demo;

CREATE EXTERNAL TABLE IF NOT EXISTS claims_demo.claims (
    claim_id STRING,
    batch_week STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/claims-demo/athena/claims/';

CREATE EXTERNAL TABLE IF NOT EXISTS claims_demo.transactions (
    claim_id STRING,
    transaction_type STRING,
    payment_amount DECIMAL(18, 2)
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/claims-demo/athena/transactions/';

SELECT
    c.batch_week AS claim_batch_week,
    COUNT(DISTINCT c.claim_id) AS claim_count,
    SUM(COALESCE(t.payment_amount, DECIMAL '0.00')) AS total_paid
FROM claims_demo.claims AS c
LEFT JOIN claims_demo.transactions AS t
    ON c.claim_id = t.claim_id AND t.transaction_type = 'PAYMENT'
GROUP BY c.batch_week
ORDER BY c.batch_week DESC;
