"""Sum cents for each week after Hadoop groups the mapper output."""

import sys
from itertools import groupby

entries = (line.rstrip("\r\n").split("\t") for line in sys.stdin)
for week, payments in groupby(entries, key=lambda entry: entry[0]):
    cents = sum(int(amount) for _, amount in payments)
    dollars, remainder = divmod(abs(cents), 100)
    sign = "-" if cents < 0 else ""
    print(f"{week}\t{sign}{dollars}.{remainder:02}")
