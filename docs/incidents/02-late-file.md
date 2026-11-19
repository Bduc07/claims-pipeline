# Drill 2: Missing transaction delivery

- **Symptom:** A claims file arrives before its expected transaction file.
- **Cause:** The delivery is incomplete at the time of ingestion.
- **Detection:** Bronze compares the landing filenames against the
  expected list and records a failed run with zero files written.
- **Fix:** Add the missing file and rerun with the same expected list.
- **Prevention:** Supply the expected filenames at orchestration time.

CI drill is `test_late_file_drill_waits_for_expected_delivery`.
