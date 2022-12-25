# diffit row

`diffit row` reporter acts on two data sources with the same schema. These are denoted as `left`
and `right`. Differences are reported at the _row_ level. Both CSV and
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) data sources are supported. 

Another way to think about the `diffit row` reporter is that identical rows from the `left` and `right` data sources
are suppressed from the output.

`diffit row` will produce a Diffit extract that is a Spark DataFrame in
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) format. The Diffit
extract can then be further analysed using other `diffit` subcommands
[see `diffit analyse`](../diffit/analyse.md) or with any other tooling that supports parquet.

A key characteristic of the Diffit extract is that it features a new column `diffit_ref`. This denotes
the source reference that has caused the row level exception. Typically, this value will be either `left` or `right`.

## Usage
``` sh
venv/bin/diffit row --help
```

``` sh title="diffit row help"
usage: diffit row [-h] [-o OUTPUT] [-d [DROP ...]] [-r RANGE] [-L LOWER] [-U UPPER] [-F] {csv,parquet} ...

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Write results to path
  -d [DROP ...], --drop [DROP ...]
                        Drop column from diffit engine
  -r RANGE, --range RANGE
                        Column to target for range filter
  -L LOWER, --lower LOWER
                        Range filter lower bound (inclusive)
  -U UPPER, --upper UPPER
                        Range filter upper bound (inclusive)
  -F, --force-range     Force string-based range filter

sub-commands:
  {csv,parquet}
    csv                 CSV row-level
    parquet             Parquet row-level
```

## CSV Data Sources
CSV files will require a schema definition. This needs to be provided as a JSON construct.

``` sh
venv/bin/diffit row csv --help
```

``` sh title="diffit row csv help"
usage: diffit row csv [-h] [-s CSV_SEPARATOR] [-E] schema left_data_source right_data_source

positional arguments:
  schema                Report CSV schema
  left_data_source      "Left" CSV source location
  right_data_source     "Right" CSV source location

options:
  -h, --help            show this help message and exit
  -s CSV_SEPARATOR, --csv-separator CSV_SEPARATOR
                        CSV separator
  -E, --csv-header      CSV contains header
```

### Example: CSV Data Sources
A sample CSV data set has been provided. The `Dummy` schema is provided as a JSON construct:
``` sh title="Example CSV JSON Schema"
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
        }
    ]
}
```

. This can be invoked as follows:
``` sh
venv/bin/diffit row csv --csv-separator ';' Dummy docker/files/data/left docker/files/data/right
```

``` sh title="diffit row csv example output"
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

## Parquet Data Sources
Take advantage of the nice features of Spark Parquet.
``` sh 
venv/bin/diffit row parquet --help
```

``` sh title="diffit row parquet help"
usage: diffit row parquet [-h] left_data_source right_data_source

positional arguments:
  left_data_source   "Left" Parquet source location
  right_data_source  "Right" Parquet source location

options:
  -h, --help         show this help message and exit
```

## Report on Subset of Spark DataFrame Columns
`diffit` can be run on a subset of DataFrame columns. This can limit the symmetric difference checker
to a reduced number of colums for more targeted, efficient processing.

To remove one or more unwanted Spark DataFrame columns use the `drop` switch. For example,
to drop `col02` from the local test sample:
 
### Example: CSV Data Sources with Column Filtering
``` sh title="diffit row csv dropping a column from the symmetric differential engine"
venv/bin/diffit row --drop col02 csv --csv-separator ';' Dummy docker/files/data/left docker/files/data/right
```

``` sh title="Result"
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

``` sh title="Dropping multiple columns from symmetric differential engine"
venv/bin/diffit row --drop col02 --drop col03 csv --csv-separator ';' Dummy docker/files/data/left docker/files/data/right
```

``` sh title="Result"
+-----+----------+
|col01|diffit_ref|
+-----+----------+
|9    |right     |
+-----+----------+
```

## Column Value Range Filtering
!!! note
    Only [`pyspark.sql.types.IntegerType`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.types.IntegerType.html) is currently supported.

Filtering can be useful to limit `diffit` to a subset of the original Spark SQL DataFrame. For example,
we can limit the test data sources under `docker/files/data/left` to remove `col01` values `3` and above as follows:

### Example: CSV Data Sources with Output Reduced through Range Filtering
``` sh title="Column range filtering"
venv/bin/diffit row --range col01 --lower 1 --upper 2 csv --csv-separator ';' Dummy docker/files/data/left docker/files/data/right
```

``` sh title="Results"
+-----+-----------+-----------+----------+
|col01|col02      |col03      |diffit_ref|
+-----+-----------+-----------+----------+
|2    |col02_val02|col03_val02|left      |
|2    |col02_valXX|col03_val02|right     |
+-----+-----------+-----------+----------
```
