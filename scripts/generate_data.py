"""Generate reproducible synthetic trades; Python standard library only."""

import argparse
import csv
import random
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path

FIELDS = ["trade_id", "timestamp", "instrument", "side", "price", "quantity", "venue", "clearing_member"]
SYMBOLS = ["ALFA", "BRAV", "CHAR", "DELT", "ECHO", "FOXT", "GOLF", "HOTL", "INDI", "JULI"]


def generate(output: Path, rows_per_day: int, seed: int) -> None:
    if rows_per_day < 1000 or rows_per_day % 1000:
        raise ValueError("rows-per-day must be a positive multiple of 1000")
    output.mkdir(parents=True, exist_ok=True)
    # Refuse to mix a new run with stale files; use a fresh directory or remove old CSVs explicitly.
    if any(output.glob("*.csv")):
        raise ValueError(f"{output} already contains CSVs; choose an empty directory")
    rng = random.Random(seed)
    counts = Counter()
    day = date(2026, 9, 1)
    for day_index in range(20):
        while day.weekday() >= 5:
            day += timedelta(days=1)
        previous = None
        with (output / f"trades-{day.isoformat()}.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            for index in range(rows_per_day):
                cents = rng.randint(1000, 50000)
                row = dict(zip(FIELDS, [
                    f"T{day_index:02d}-{index:05d}",
                    (datetime.combine(day, time(8)) + timedelta(seconds=rng.randrange(8 * 3600)))
                    .strftime("%Y-%m-%dT%H:%M:%S"),
                    rng.choice(SYMBOLS), rng.choice(["BUY", "SELL"]),
                    f"{cents // 100}.{cents % 100:02d}", rng.randint(1, 1000),
                    rng.choice(["VENUE_A", "VENUE_B", "VENUE_C"]),
                    rng.choice(["CM01", "CM02", "CM03", "CM04", "CM05"]),
                ]))
                kind = {100: "invalid_price", 200: "invalid_quantity", 300: "duplicate_trade_id",
                        400: "invalid_timestamp"}.get(index % 1000, "accepted")
                if kind == "invalid_price":
                    row["price"] = ""
                elif kind == "invalid_quantity":
                    row["quantity"] = -row["quantity"]
                elif kind == "duplicate_trade_id":
                    row = previous.copy()
                elif kind == "invalid_timestamp":
                    row["timestamp"] = day.strftime("%d/%m/%Y") + " 09:30:00"
                writer.writerow(row)
                counts[kind] += 1
                previous = row
        day += timedelta(days=1)
    print(f"Generated {sum(counts.values()):,} rows in 20 CSVs (seed={seed}).")
    for kind, count in sorted(counts.items()):
        print(f"  {kind}: {count:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument("--rows-per-day", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate(args.output, args.rows_per_day, args.seed)
