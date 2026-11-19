# Incident 0: Unexpected transaction quarantine

- **Symptom:** The first silver run quarantined 48,212 transaction rows.
- **Cause:** The first rule required `AMOUNT` on every transaction type.
- **Detection:** Compared blank amounts by type against Synthea's export
  behavior. Payments and transfers out accounted for 48,211 expected blanks.
- **Fix:** Required numeric amounts for charges and transfers in and
  allowed intentional blanks. The rerun kept 89,473 rows and quarantined
  the one injected invalid amount.
- **Prevention:** Keep the required types in YAML and test blank, valid,
  and malformed amounts.
