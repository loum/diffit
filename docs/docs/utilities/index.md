# Usage

`diffit` provides a command line utility that you can use to invoke the Apache Spark-based
symmetric differential engine.

``` sh
venv/bin/diffit --help
```

``` sh title="diffit usage message."
 Usage: diffit [OPTIONS] COMMAND [ARGS]...

 Diff-it Data Diff tool.

╭─ Options ─────────────────────────────────────────────────────────────────────────────╮
│ --quiet                        Disable logs to screen (to log level "ERROR").         │
│ --driver-memory  -M      TEXT  Set Spark driver memory. [default: 1g]                 │
│ --help                         Show this message and exit.                            │
╰───────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ────────────────────────────────────────────────────────────────────────────╮
│ analyse        List rows that are unique to the nominated source DataFrame.           │
│ columns        Display the columns that are different.                                │
│ convert        Convert CSV to Apache Parquet.                                         │
│ row            Filter out identical rows from the left and right data sources.        │
╰───────────────────────────────────────────────────────────────────────────────────────╯
```
