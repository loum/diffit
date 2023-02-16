# parquet

Take advantage of the nice features of Spark Parquet.

``` sh 
venv/bin/diffit row parquet --help
```

``` sh title="diffir row parquet usage message."
 Usage: diffit row parquet [OPTIONS]

 Spark DataFrame row-level diff from Spark Parquet source data.

╭─ Options ──────────────────────────────────────────────────────────────────────────────────╮
│    --drop          -d      TEXT     Drop column from diffit engine                         │
│    --range-column  -r      TEXT     Column to target for range filter                      │
│    --lower         -L      INTEGER  Range filter lower bound (inclusive)                   │
│    --upper         -U      INTEGER  Range filter upper bound (inclusive)                   │
│    --force-range   -F               Force (cast) string-based range column filter          │
│ *  --left          -l      TEXT     Path to left data source [required]                    │
│ *  --right         -r      TEXT     Path to right data source [required]                   │
│    --parquet-path          TEXT     Path to Spark Parquet: output                          │
│    --help                           Show this message and exit.                            │
╰────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Report on Subset of Spark DataFrame Columns
`diffit` can be run on a subset of DataFrame columns. This can limit the symmetric difference checker
to a reduced number of colums for more targeted, efficient processing.

To remove one or more unwanted Spark DataFrame columns use the `drop` switch.
 
### Example: Parquet Data Sources with Column Filtering

To drop `col02` from the local test sample:

``` sh title="diffit row Parquet dropping a column from the symmetric differential engine."
venv/bin/diffit row parquet --drop col02 --left docker/files/parquet/left --right docker/files/parquet/right
```

``` sh title="Result."
+-----+-----------+----------+
|col01|col03      |diffit_ref|
+-----+-----------+----------+
|8    |col03_val08|left      |
|8    |col03_valYY|right     |
|9    |col03_val09|right     |
+-----+-----------+----------+
```

Multiple columns can be added to the `drop` switch separated by spaces. For example:
``` sh
... --drop col01 --drop col02 ... --drop <col_n> 
```

``` sh title="Dropping multiple columns from symmetric differential engine."
venv/bin/diffit row parquet --drop col02 --drop col03 --left docker/files/parquet/left --right docker/files/parquet/right
```

``` sh title="Result."
+-----+----------+
|col01|diffit_ref|
+-----+----------+
|9    |right     |
+-----+----------+
```

## Column Value Range Filtering
!!! note
    Only [`pyspark.sql.types.IntegerType`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.types.IntegerType.html) is currently supported.

Column range filtering can be useful to limit `diffit` to a subset of the original Spark SQL DataFrame.

### Example: CSV Data Sources with Output Reduced through Range Filtering

To limit the test data sources under `docker/files/data/left` to remove `col01` values `3` and above as follows:

``` sh title="Column range filtering."
venv/bin/diffit row parquet --range-column col01 --lower 1 --upper 2 --left docker/files/parquet/left --right docker/files/parquet/right
```

``` sh title="Result."
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|2    |col02_val02|col03_val02|left      |
|2    |col02_valXX|col03_val02|right     |
+-----+-----------+-----------+----------
```
