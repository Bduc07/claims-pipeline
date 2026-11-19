# Drill 3: Corrupted transaction amount

- **Symptom:** A charge contains `INVALID` instead of a numeric amount.
- **Cause:** The sender supplied malformed data.
- **Detection:** The blocking amount rule records the failure in
  `dq_results`. The linked silver run reports zero rows published.
- **Fix:** Correct the value and rerun with the same blocking rule.
  Verify that the corrected run succeeds and has zero quarantined rows.
- **Prevention:** Keep the amount validation and audit records available
  for investigation.

CI drill is `test_pipeline_runs_persists_success_and_block`.
