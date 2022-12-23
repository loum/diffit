# diffit row

`diffit row` reporter acts on two similarly constructed
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) output sources. These
are denoted as `left` and `right`. Differences are reported at the _row_ level.

Another way to think about the `diffit row` reporter is that identical rows from the `left` and `right` data sources
are suppressed from the output.

`diffit row` will produce a Diffit extract that is a Spark DataFrame in
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) format. The Diffit
extract can then be further analysed using other `diffit` subcommands
[see `diffit analyse`](../utilities/analyse.md) or with any other tooling that supports parquet.

A key characteristic of the Diffit extract is that it features a new column `diffit_ref`. This denotes
the source reference that has caused the row level exception. Typically, this value will be either `left` or `right`.

## Usage
``` sh
venv/bin/diffit row --help
```

``` sh
usage: diffit row [-h] [-o OUTPUT] [-d [DROP ...]] [-r RANGE] [-L LOWER] [-U UPPER] [-F]
                  schema left_data_source right_data_source

positional arguments:
  schema                Report schema
  left_data_source      "Left" DataFrame source location
  right_data_source     "Right" DataFrame source location

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
```

## Example
A sample data set has been provided for the `Dummy` schema. This can be invoked as follows:
``` sh
venv/bin/diffit row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
```

Use the local `pyspark` shell to view the results:
``` sh
make pyspark
```

``` sh
>>> df = spark.read.parquet('/tmp/out')
>>> df.orderBy('col01').show()
+-----+-----------+----------+
|col01|      col02|diffit_ref|
+-----+-----------+----------+
|    2|col02_val02|      left|
|    2|col02_valXX|     right|
|    9|col02_val09|     right|
+-----+-----------+----------+
```

## Example: Report on Subset of Spark DataFrame Columns
`diffit` can be run on a subset of DataFrame columns. This can limit the symmetric difference checker
to a reduced number of colums for more targeted, efficient processing.

To remove one or more unwanted Spark DataFrame columns use the `drop` switch. For example,
to drop `col02` from the local test sample:
``` sh
venv/bin/diffit row --output /tmp/out --drop col02 -- Dummy docker/files/data/left docker/files/data/right
```

To view the results:
``` sh
make pyspark
```

``` sh
>>> df = spark.read.parquet('/tmp/out')
>>> df.show()
+-----+----------+
|col01|diffit_ref|
+-----+----------+
|    9|     right|
+-----+----------+
```

Multiple columns can be added to the `drop` switch separated by spaces. For example:
``` sh
... --drop col01 col02 ... col<n> --
```

### Example: Column Value Range Filtering
!!! note
    Only [`pyspark.sql.types.IntegerType`](pyspark.sql.types.IntegerType)  is currently supported.

Filtering can be useful to limit `diffit` to a subset of the original Spark SQL DataFrame. For example,
we can limit the test data sources under `docker/files/data/left` to remove `col01` values `3` and above as follows:

``` sh
venv/bin/diffit row --output /tmp/out --range col01 --lower 1 --upper 2 -- Dummy docker/files/data/left docker/files/data/right
```

To view the results:
``` sh
make pyspark
```

``` sh
>>> df = spark.read.parquet('/tmp/out')
>>> df.orderBy('col01').show()
+-----+-----------+----------+
|col01|      col02|diffit_ref|
+-----+-----------+----------+
|    2|col02_valXX|     right|
|    2|col02_val02|      left|
+-----+-----------+----------+
```
