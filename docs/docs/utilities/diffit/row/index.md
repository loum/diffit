# Usage

`diffit row` reporter acts on two data sources with the same schema. These are denoted as `left`
and `right`. Differences are reported at the _row_ level. Both CSV and
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) data sources are supported. 

Another way to think about the `diffit row` reporter is that identical rows from the
`left` and `right` data sources are suppressed from the output.

`diffit row` will produce a Diffit extract that is a Spark DataFrame in
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) format. The Diffit
extract can then be further analysed using other `diffit` subcommands
[see `diffit analyse`](../analyse/index.md) or with any other tooling that supports parquet.

A key characteristic of the Diffit extract is that it features a new column `diffit_ref`. This denotes
the source reference that has caused the row level exception. Typically, this value will be either `left` or `right`.

``` sh
venv/bin/diffit row --help
```

``` sh title="diffit row usage message."
 Usage: diffit row [OPTIONS] COMMAND [ARGS]...

 Filter out identical rows from the left and right data sources.

╭─ Options ────────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                      │
╰──────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────╮
│ csv            Spark DataFrame row-level diff from CSV source data.              │
│ parquet        Spark DataFrame row-level diff from Spark Parquet source data.    │
╰──────────────────────────────────────────────────────────────────────────────────╯
```
