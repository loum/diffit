# altered

`diffit analyse altered` provides a way to report on `diffit` engine extract rows that appear in both target
data sources and flagged as being different.

``` sh
venv/bin/diffit analyse altered --help
```

``` sh title="diffit analyse altered usage message."
 Usage: diffit analyse altered [OPTIONS] PARQUET_PATH

 Spark DataFrame list rows source data.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    parquet_path      TEXT  Path to Spark Parquet: input [required]                                         │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  --key           -k      TEXT     Analysis column to act as a unique constraint [default: None] [required] │
│    --range-column  -r      TEXT     Column to target for range filter                                        │
│    --lower         -L      INTEGER  Range filter lower bound (inclusive)                                     │
│    --upper         -U      INTEGER  Range filter upper bound (inclusive)                                     │
│    --force-range   -F               Force (cast) string-based range column filter                            │
│    --descending    -D               Change output ordering to descending                                     │
│    --counts-only   -C               Only output counts                                                       │
│    --hits          -H      INTEGER  Rows to display [default: 20]                                            │
│    --help                           Show this message and exit.                                              │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Example

!!! note
    The following examples source sample diffit engine output data that can be found at
    `docker/files/parquet/analysis`.

``` sh title="diffit analyse altered command with schema column col01 as the unique constraint."
venv/bin/diffit analyse altered --key col01 docker/files/parquet/analysis
```

``` sh title="Result."
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|2    |col02_val02|col03_val02|left      |
|2    |col02_valXX|col03_val02|right     |
|8    |col02_val08|col03_val08|left      |
|8    |col02_val08|col03_valYY|right     |
+-----+-----------+-----------+----------+
```

To reverse the order of the output (based on `key` column `col01`):

``` sh title="diffit analyse altered command reverse ordering."
venv/bin/diffit analyse altered --key col01 --descending docker/files/parquet/analysis
```

``` sh title="Result."
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|8    |col02_val08|col03_val08|left      |
|8    |col02_val08|col03_valYY|right     |
|2    |col02_val02|col03_val02|left      |
|2    |col02_valXX|col03_val02|right     |
+-----+-----------+-----------+----------+
```

To only report on the counts:

``` sh title="diffit analyse altered command counts only output."
venv/bin/diffit analyse altered --key col01 --counts-only docker/files/parquet/analysis
```

``` sh title="Result."
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
...
2023-02-16 13:18:44 logga [INFO]: Reading Parquet data from "docker/files/parquet/analysis"
2023-02-16 13:18:48 logga [INFO]: Altered rows analysis count: 2
```
```
