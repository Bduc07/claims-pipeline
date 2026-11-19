# Claims pipeline runbook

## Find the failed stage

1. For bronze, read `bronze/pipeline_runs` with Spark and inspect the
   latest `status`, `error_type`, `files_found`, `files_written`, and
   `files_skipped`. The Colab path starts with `/content/`.
2. For silver, read `silver/pipeline_runs`, find the run ID, and filter
   `silver/dq_results` by that ID. Inspect the table under
   `silver/quarantine` for the rejected records.
3. For gold, open Snowflake's query history and inspect the failed SQL
   statement. Check staging counts and duplicate merge keys before
   rerunning the relevant statements in `sql/gold_pipeline.sql`.

## Recover the delivery

| Symptom | Recovery |
| --- | --- |
| Expected file missing | Confirm the delivery list, obtain the missing file, then rerun bronze with the same expected filenames. |
| New CSV column | Compare headers, read both bronze partitions with schema merging, and verify the resulting columns before publishing silver. |
| Blocked silver run | Use the rule ID and quarantine to locate the bad records. Correct the upstream data, rebuild the affected input in a fresh output location if the original was already ingested, and rerun the same rules. |

Verify a successful run and reconcile counts before continuing to gold.
Save the run ID and the validation result in the incident notes. Keep
CI evidence or export the run summaries if you need them after the Colab
runtime ends.
