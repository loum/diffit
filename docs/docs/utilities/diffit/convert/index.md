# Usage

Convert as data source to
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
to with a given compression (default `snappy`).

!!! note
    Only CSV format is supported at this time.

``` sh 
venv/bin/diffit convert --help
```

``` sh title="diffit convert CSV to Spark Parquet usage message."
 Usage: diffit convert [OPTIONS] COMMAND [ARGS]...

 Convert CSV to Apache Parquet.

╭─ Options ──────────────────────────────────────────────────╮
│ --help          Show this message and exit.                │
╰────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────╮
│ csv           Convert CSV to Apache Parquet.               │
╰────────────────────────────────────────────────────────────╯
```
