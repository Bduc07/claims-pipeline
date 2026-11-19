"""Package Synthea claims and their transactions into weekly deliveries."""

import argparse
import csv
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


def write_csv(path, columns, rows):
    with path.open("x", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--period", choices=["week"], default="week")
    parser.add_argument("--last", type=int, default=52)
    args = parser.parse_args()
    if args.last < 1:
        parser.error("--last must be positive")

    claims_by_week = defaultdict(list)
    with (args.src / "claims.csv").open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        claim_columns = next(reader)
        service_date = claim_columns.index("SERVICEDATE")
        claim_id = claim_columns.index("Id")
        for row in reader:
            day = date.fromisoformat(row[service_date][:10])
            claims_by_week[day - timedelta(days=day.weekday())].append(row)
    if not claims_by_week:
        parser.error("claims.csv must contain claims")
    first_week = max(claims_by_week) - timedelta(weeks=args.last - 1)
    batches = sorted((week, rows) for week, rows in claims_by_week.items() if week >= first_week)
    claim_weeks = {row[claim_id]: week for week, rows in batches for row in rows}
    transactions_by_week = defaultdict(list)
    with (args.src / "claims_transactions.csv").open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        transaction_columns = next(reader)
        transaction_claim_id = transaction_columns.index("CLAIMID")
        for row in reader:
            week = claim_weeks.get(row[transaction_claim_id])
            if week is not None:
                transactions_by_week[week].append(row)

    landing = args.out / "landing"
    answer_key = args.out / "answer_key.csv"
    if answer_key.exists() or (landing.exists() and any(landing.iterdir())):
        parser.error("the output already contains batch files or an answer key")
    landing.mkdir(parents=True, exist_ok=True)
    claim_count = sum(len(rows) for _, rows in batches)
    print(f"Keeping {claim_count} claims in {len(batches)} batches.", flush=True)

    anomalies = []
    for index, (week, batch) in enumerate(batches):
        columns = claim_columns
        transaction_batch = transactions_by_week[week]
        claim_file = f"claims_{week}.csv"
        transaction_file = f"transactions_{week}.csv"
        if index == 4:
            batch[0][claim_columns.index("STATUS1")] = "UNKNOWN"
            anomalies.append((week, claim_file, "invalid_status", batch[0][claim_id]))
        if index == 8 and transaction_batch:
            transaction_batch[0][transaction_columns.index("AMOUNT")] = "INVALID"
            anomalies.append(
                (week, transaction_file, "invalid_amount",
                 transaction_batch[0][transaction_columns.index("ID")])
            )
        if index == 16:
            columns = [*claim_columns, "SOURCE_SYSTEM"]
            batch = [[*row, "SYNTHEA"] for row in batch]
            anomalies.append((week, claim_file, "added_column", "SOURCE_SYSTEM"))

        write_csv(landing / claim_file, columns, batch)
        write_csv(landing / transaction_file, transaction_columns, transaction_batch)
        if index in (5, 25, 45):
            resend = f"claims_{week}_resend.csv"
            write_csv(landing / resend, columns, batch)
            anomalies.append((week, resend, "duplicate_delivery", claim_file))

    write_csv(answer_key, ["batch_id", "source_file", "issue", "detail"], anomalies)


if __name__ == "__main__":
    main()
