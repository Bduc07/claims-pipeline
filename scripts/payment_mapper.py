"""Emit claim batch weeks and integer cents for Hadoop Streaming."""

import sys
from decimal import Decimal

for line in sys.stdin:
    week, payment = line.rstrip("\r\n").split("\t")
    cents = Decimal(payment) * 100
    if cents != cents.to_integral_value():
        raise ValueError("Payment amount must be expressible in cents.")
    print(f"{week}\t{int(cents)}")
