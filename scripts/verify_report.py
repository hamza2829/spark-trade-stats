"""Independently reconcile generated CSV input against Spark's written daily report."""

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path


def verify(raw_path: Path, output: Path, threshold: Decimal) -> None:
    seen = set()
    counts = Counter()
    daily = defaultdict(lambda: {"trade_count": 0, "total_quantity": 0, "notional": Decimal(0)})
    members = defaultdict(Decimal)
    for file in sorted(raw_path.glob("*.csv")):
        with file.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                counts["input_rows"] += 1
                # This small reference checker targets the generator's four deliberate error types.
                try:
                    datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S")
                    price, quantity = Decimal(row["price"]), int(row["quantity"])
                    valid = price.is_finite() and price > 0 and quantity > 0
                except (ValueError, ArithmeticError):
                    valid = False
                if not valid or row["trade_id"] in seen:
                    counts["rejected_rows"] += 1
                    continue
                seen.add(row["trade_id"])
                counts["accepted_rows"] += 1
                day = row["timestamp"][:10]
                amount = price * quantity
                daily[day]["trade_count"] += 1
                daily[day]["total_quantity"] += quantity
                daily[day]["notional"] += amount
                members[(day, row["clearing_member"])] += amount

    parts = list((output / "summary").glob("part-*.csv"))
    assert len(parts) == 1, "Expected one summary CSV"
    with parts[0].open(newline="", encoding="utf-8") as handle:
        report = list(csv.DictReader(handle))
    assert len(report) == len(daily) == 20
    assert len({row["date"] for row in report}) == len(report)
    for row in report:
        expected = daily[row["date"]]
        assert int(row["trade_count"]) == expected["trade_count"], row
        assert int(row["total_quantity"]) == expected["total_quantity"], row
        assert Decimal(row["notional"]) == expected["notional"], row
        flags = sum(amount > threshold for (day, _), amount in members.items() if day == row["date"])
        assert int(row["flagged_members"]) == flags, row
    audit_parts = list((output / "reconciliation").glob("part-*.json"))
    assert len(audit_parts) == 1
    audit = json.loads(audit_parts[0].read_text(encoding="utf-8"))
    assert all(audit[key] == value for key, value in counts.items())
    assert audit["balanced"] and counts["input_rows"] == counts["accepted_rows"] + counts["rejected_rows"]
    print("Independent CSV/Decimal verification passed for all 20 daily summary rows.")
    print(json.dumps(dict(counts), sort_keys=True))
    print(f"Total quantity: {sum(day['total_quantity'] for day in daily.values()):,}")
    print(f"Total notional: {sum(day['notional'] for day in daily.values()):,.2f}")
    print(f"Flagged member-days: {sum(amount > threshold for amount in members.values())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, default=Path("output"))
    parser.add_argument("--threshold", type=Decimal, default=Decimal("250000000.00"))
    args = parser.parse_args()
    verify(args.raw, args.output, args.threshold)
