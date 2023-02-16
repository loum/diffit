# distinct

`diffit analyse distinct` provides a way to report on rows from a `diffit` engine extract that
only appear in either one of the `left` or `right` target data sources.

``` sh
venv/bin/diffit analyse distinct --help
```
``` sh title="diffit analyse distinct usage message."
 Usage: diffit analyse distinct [OPTIONS] PARQUET_PATH

 Spark DataFrame list rows source data.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    parquet_path      TEXT  Path to Spark Parquet: input [required]                                     │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────╮
│    --orientation   -O      [left|right]  Limit analysis orientation to either "left" or "right"          │
│ *  --key           -k      TEXT          Analysis column to act as a unique constraint [default: None]   │
│                                          [required]                                                      │
│    --descending    -D                    Change output ordering to descending                            │
│    --counts-only   -C                    Only output counts                                              │
│    --hits          -H      INTEGER       Rows to display [default: 20]                                   │
│    --help                                Show this message and exit.                                     │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Example: Analyse Rows Unique to Each Spark DataFrame

!!! note
    The following examples source sample `diffit` engine output data that can be found at
    `docker/files/parquet/analysis`.

``` sh title="The key setting, col01, acts as the GROUP BY predicate."
venv/bin/diffit analyse distinct --key col01 docker/files/parquet/analysis
```

``` sh title="Combined diffit analyse distinct output."
### Analysing distinct rows from "left" source DataFrame
+-----+-----+-----+----------+
|col01|col02|col03|diffit_ref|
+-----+-----+-----+----------+
+-----+-----+-----+----------+

### Analysing distinct rows from "right" source DataFrame
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|9    |col02_val09|col03_val09|right     |
+-----+-----------+-----------+----------+
```

A Diffit extract can be limited with the `--orientation` switch. For example, to only show
distinct Diffit extract records from the `right` data source:

``` sh title="Analysing distinct rows from right source Spark DataFrame."
venv/bin/diffit analyse distinct --orientation right --key col01 docker/files/parquet/analysis
```

``` sh title="Result."
### Analysing distinct rows from "right" source DataFrame
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|9    |col02_val09|col03_val09|right     |
+-----+-----------+-----------+----------+
```
