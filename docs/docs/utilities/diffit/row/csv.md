# csv

CSV source data files will require a schema definition. This needs to be provided as a JSON construct.

``` sh
venv/bin/diffit row csv --help
```

``` sh title="diffit row csv usage message."
 Usage: diffit row csv [OPTIONS]

 Spark DataFrame row-level diff from CSV source data.

╭─ Options ──────────────────────────────────────────────────────────────────────────────────╮
│    --add            -a      TEXT     Add column to the diffit engine                       │
│    --drop           -d      TEXT     Drop column from diffit engine                        │
│    --range-column   -r      TEXT     Column to target for range filter                     │
│    --lower          -L      INTEGER  Range filter lower bound (inclusive)                  │
│    --upper          -U      INTEGER  Range filter upper bound (inclusive)                  │
│    --force-range    -F               Force (cast) string-based range column filter         │
│    --csv-separator  -s      TEXT     CSV separator [default: ,]                            │
│    --csv-header     -E               CSV contains header                                   │
│ *  --json-schema    -J      TEXT     Path to CSV schema in JSON format [required]          │
│ *  --left           -l      TEXT     Path to left data source [required]                   │
│ *  --right          -r      TEXT     Path to right data source [required]                  │
│    --parquet-path           TEXT     Path to Spark Parquet: output                         │
│    --help                            Show this message and exit.                           │
╰────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Example: CSV Data Sources

### Sample CSV schema
Save the following sample JSON schema definition to `/tmp/Dummy.json`:
``` sh title="Example CSV JSON Schema."
{
    "type": "struct",
    "fields": [
        {
            "name" : "col01",
            "type" : "integer",
            "nullable": true,
            "metadata": {}
        },
        {
            "name": "col02",
            "type": "string",
            "nullable": true,
            "metadata": {}
        },
        {
            "name": "col03",
            "type": "string",
            "nullable": true,
            "metadata": {}
        }
    ]
}
```

Next, run the CSV row comparitor:
``` sh title="diffit row csv command."
venv/bin/diffit row csv --csv-separator ';' --json-schema /tmp/Dummy.json --left docker/files/data/left --right docker/files/data/right
```

``` sh title="diffit row csv example output."
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|2    |col02_val02|col03_val02|left      |
|8    |col02_val08|col03_val08|left      |
|9    |col02_val09|col03_val09|right     |
|2    |col02_valXX|col03_val02|right     |
|8    |col02_val08|col03_valYY|right     |
+-----+-----------+-----------+----------+
```

## Report on Subset of Spark DataFrame Columns

`diffit` can be run on a subset of DataFrame columns. This can limit the symmetric difference checker
to a reduced number of colums for more targeted, efficient processing.

To remove one or more unwanted Spark DataFrame columns use the `drop` switch.
 
### Example: CSV Data Sources with Column Filtering

The following example will drop `col02` from the test sample:

``` sh title="diffit row csv dropping a column from the symmetric differential engine."
venv/bin/diffit row csv --drop col02 --csv-separator ';' --json-schema /tmp/Dummy.json --left docker/files/data/left --right docker/files/data/right
```

``` sh title="Column filtering result."
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
venv/bin/diffit row csv --drop col02 --drop col03 --csv-separator ';' --json-schema /tmp/Dummy.json --left docker/files/data/left --right docker/files/data/right
```

``` sh title="Multiple column filtering result."
+-----+----------+
|col01|diffit_ref|
+-----+----------+
|9    |right     |
+-----+----------+
```

## Column Value Range Filtering
!!! note
    Only [`pyspark.sql.types.IntegerType`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.IntegerType.html) is currently supported.

Value range filtering can be useful to limit `diffit` to a subset of rows from the original
Spark SQL DataFrame.

### Example: CSV Data Sources with Output Reduced through Range Filtering

This example limits the test data sources under `docker/files/data/left` by removing
`col01` values `3` and above:

``` sh title="Column range filtering."
venv/bin/diffit row csv --range-column col01 --lower 1 --upper 2 --csv-separator ';' --json-schema /tmp/Dummy.json --left docker/files/data/left --right docker/files/data/right
```

``` sh title="Result."
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|2    |col02_val02|col03_val02|left      |
|2    |col02_valXX|col03_val02|right     |
+-----+-----------+-----------+----------
```
