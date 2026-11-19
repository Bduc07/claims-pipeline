# Security and data handling

This project uses synthetic Synthea records to demonstrate controls for
healthcare-like data. It is a portfolio project and has no HIPAA certification.

## Sensitive fields

| Field | Handling |
| --- | --- |
| `PATIENTID` | Raw in landing and bronze. Replaced with an HMAC-SHA-256 token in silver and quarantine. The selected claims export uses that token as `PATIENT_KEY`. |
| Transaction `PATIENTINSURANCEID` | Treated as a member identifier. Removed from silver and quarantine. |
| Transaction `NOTES` and `LINENOTE` | Removed from silver and quarantine because free text can contain identifiers. |
| Patient names, birth dates, addresses, SSNs, and payer owner names | Optional reference tables are used for validation. Their non-key fields do not enter the claim outputs. |
| Claim and provider IDs, payer identifiers, service dates, and financial values | Kept where needed for joins, validation, and reporting. Limit access because these values can still be sensitive in real data. |

Keep landing, bronze, and raw reference files in restricted storage. Treat
quarantine as sensitive even after tokenization. Tokens are pseudonymous
identifiers. People with the key can compute tokens for known patient IDs.

## Masking key

Set `CLAIMS_PIPELINE_HMAC_KEY` from a secret manager or a masked prompt.
Use a randomly generated secret, such as 32 random bytes encoded as hex.
Silver fails if the key is unavailable or shorter than 32 UTF-8 bytes.
The Colab notebook prompts once per runtime, and CI creates an ephemeral
key for each test. Keep the key out of Git, outputs, and logs. Restrict
access to the Spark driver and executors because the masking function
uses the key on workers.

Use the same key across claims and transactions and across runs that need
to join by patient. When rotating the key, rebuild the affected silver
and gold snapshots together. Different keys produce different tokens.

## Snowflake roles and audit

After creating the staging and gold tables, an administrator runs
[`sql/security.sql`](sql/security.sql). It grants the loader access to
read and append to the existing staging tables and to insert or update
the two gold tables. The reporter receives read access to the two gold
tables. The administrator can assign the roles to the appropriate users.

For a role-specific check, use the assigned role with secondary roles
disabled. Verify that the reporter can run the gold report and that a
staging query fails. Verify that the loader can run the two gold
`MERGE` statements with the existing tables. Bootstrap DDL stays with
the administrator.

Before each load or merge, set `QUERY_TAG` in the loader's session to
`claims_pipeline_load:` followed by the destination table name. For
example, for the claims merge run
`ALTER SESSION SET QUERY_TAG = 'claims_pipeline_load:GOLD.CLAIMS';`.
The audit query returns the user, role, query ID, destination tag, status,
and row counts. Use the query ID to investigate a load in Snowsight.
