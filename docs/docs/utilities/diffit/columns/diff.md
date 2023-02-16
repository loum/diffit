# diff

Report on Spark DataFrame column/value pair differences.

Diffit extracts can be large, based on the number of exceptions detected. `diffit columns` allows
you to report on a specific key/value pairing for targeted analysis.

Output is displayed as a JSON construct.

``` sh
venv/bin/diffit columns diff --help
```

``` sh title="diffit columns diff usage message."
 Usage: diffit columns diff [OPTIONS] PARQUET_PATH

 Analyse altered column name differences.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    parquet_path      TEXT  Path to Spark Parquet: input [required]                                         │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --key    -k      TEXT  Analysis column to act as a unique constraint [default: None] [required]           │
│ *  --value  -V      TEXT  Unique constraint column value to filter against [required]                        │
│    --help                 Show this message and exit.                                                        │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Example

!!! note
    The following examples source sample diffit engine output data that can be found at
    `docker/files/parquet/analysis`.

The `diffit` engine extract at `docker/files/parquet/analysis` features:

- A schema column `col01` acting as the unique constraint.
- A key column value of `2` as a filter.

``` sh title="diffit columns diff filter for key:value pair col01:2"
venv/bin/diffit columns diff --key col01 --value 2 docker/files/parquet/analysis
```

``` sh title="Result."
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
...
2023-02-16 14:20:33 logga [INFO]: Reading Parquet data from "docker/files/parquet/analysis"
2023-02-16 14:20:38 logga [INFO]: key|val:
[
    {
        "col02": "col02_val02"
    }
]
```

!!! note
    The value listed under the key/value result is a taken from the `left` orinented data source. 
