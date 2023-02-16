# csv

Convert as CSV data source with schema file to
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
to with a given compression (default `snappy`)

``` sh
venv/bin/diffit convert csv --help
```

``` sh title="diffit convert csv usage message."
i Usage: diffit convert csv [OPTIONS] PARQUET_PATH

 Convert CSV to Apache Parquet.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    parquet_path      TEXT  Path to Spark Parquet: output [required]                                    │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --json-schema       -J      TEXT                                   Path to CSV schema in JSON format  │
│                                                                       [required]                         │
│ *  --csv-data          -C      TEXT                                   Path to CSV data source [required] │
│    --compression-type  -Z      [brotli|gzip|lz4|lzo|none|snappy|unco  Compression type [default: snappy] │
│                                mpressed|zstd]                                                            │
│    --num-partitions    -N      INTEGER                                Number of partitions [default: 8]  │
│    --help                                                             Show this message and exit.        │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Example

Convert sample CSV into Apache Parquet. The following [CSV sample schema](../row/csv.md#sample-csv-schema)
is re-used from previous examples.

``` sh
venv/bin/diffit convert csv --csv-separator ";" --json-schema /tmp/Dummy.json --csv-data docker/files/data/left /tmp/converted
```
