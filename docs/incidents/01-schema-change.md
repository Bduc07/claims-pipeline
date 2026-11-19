# Drill 1: Added source column

- **Symptom:** A weekly claims file arrives with an extra `SOURCE_SYSTEM`
  column.
- **Cause:** The sender added a column between deliveries.
- **Detection:** Compare the file headers and inspect the merged bronze
  schema.
- **Fix:** Keep the files as separate Parquet outputs. Read completed
  outputs with schema merging. Older records have a null in the new field.
- **Prevention:** Keep a regression fixture with both file shapes.

CI drill is `test_schema_change_drill_preserves_added_column`.
