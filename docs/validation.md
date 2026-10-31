# Local validation — 31 October 2026

Executed on Windows x64 using Java 17.0.12, sbt 1.10.7, Scala 2.13.16, Spark 3.5.7 and Python 3.11.0. The job used `local[*]`; tests used `local[2]`. File I/O used the Java filesystem adapter configured in `LocalSpark.scala`. These are local measurements against generated data.

Commands, from the project root (Python 3.11 was invoked using its installed executable):

```powershell
python scripts/generate_data.py
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-17'
.\scripts\sbt.ps1 test 'run config/application.conf'
python scripts/verify_report.py
```

The final combined sbt invocation exited successfully. ScalaTest reported:

```text
Total number of tests run: 29
Suites: completed 1, aborted 0
Tests: succeeded 29, failed 0, canceled 0, ignored 0, pending 0
All tests passed.
```

The full job processed 20 CSV files, covering 20 weekdays from 2026-09-01 to 2026-09-28. Its reconciliation and Parquet read-back checks passed:

| Check | Result |
| --- | ---: |
| Input CSV rows, excluding headers | 200,000 |
| Accepted unique valid trades | 199,200 |
| Rejected rows | 800 |
| Accepted + rejected | 200,000 |
| Rejected: duplicate_trade_id | 200 |
| Rejected: invalid_price | 200 |
| Rejected: invalid_quantity | 200 |
| Rejected: invalid_timestamp | 200 |
| Accepted total quantity | 99,694,970 |
| Accepted total notional | 25,393,967,465.67 |
| Instrument/day rows | 200 |
| Clearing member/day rows | 100 |
| Date partitions in each daily Parquet table | 20 |
| Summary CSV part files | 1 |
| Summary data rows | 20 |
| Member-days above 250,000,000.00 | 72 |

Every date had 9,960 accepted trades and 10 instruments. The sum of the written instrument trade counts and the written member trade counts each equaled 199,200. Written instrument quantity/notional and member notional matched the aggregates of the accepted trades. The written rejected Parquet contained 800 records.

An actual instrument/day result, read back from Parquet:

```csv
date,instrument,trade_count,total_quantity,notional,vwap,high,low,buy_count,sell_count,buy_sell_ratio
2026-09-01,ALFA,979,492590,128104358.02,260.062847,499.99,10.05,485,494,0.9817813765182186
```

The independent Python CSV/Decimal calculation reported:

```text
Independent CSV/Decimal verification passed for all 20 daily summary rows.
{"accepted_rows": 199200, "input_rows": 200000, "rejected_rows": 800}
Total quantity: 99,694,970
Total notional: 25,393,967,465.67
Flagged member-days: 72
```

The checker compared daily counts, quantities, decimal notionals and member flag counts for every summary date, and checked the JSON reconciliation against its own input counts.

[sample-summary.csv](sample-summary.csv) and [sample-reconciliation.json](sample-reconciliation.json) were copied from this successful run. Full local logs are gitignored in `logs/validation-run2.log` and `logs/independent-verification.log`; generated data and complete Parquet outputs are also gitignored. The initial run exposed the default Windows filesystem's missing native helper; the final run and tests passed with the Java adapter. No native binaries were installed.

GitHub Actions is configured to run `sbt test` on Java 17. This record covers local validation; remote workflow results are available from the repository's Actions tab.
