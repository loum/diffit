# Diff-it: Data Differ
- [Overview](#Overview)
- [Prerequisites](#Prerequisites)
  - [Upgrading GNU Make (macOS)](#Upgrading-GNU-Make-macOS)
- [Getting Started](#Getting-Started)
  - [Creating the Local Environment](#Creating-the-Local-Environment)
    - [Local Environment Maintenance](#Local-Environment-Maintenance)
- [Help](#Help)
- [Running the Test Harness](#Running-the-Test-Harness)
- [Docker Image Management](#Docker-Image-Management)
  - [Image Build](#Image-Build)
  - [Validate the Image Build](#Validate-the-Image-Build)
  - [Image Searches](#Image-Searches)
  - [Docker Image Version Tag](#Docker-Image-Version-Tag)
- [Automating Builds with BuildKite](#Automating-Builds-with-BuildKite)
- [`diffit` Command](#diffit-Command)
- [Useful `make` Commands](#Useful-make-Commands)
- [FAQs](#FAQs)

## Overview
The data diffit will report differences between two data sets with the same schema.

## Prerequisites
- [GNU make](https://www.gnu.org/software/make/manual/make.html)
- Python 3.10 Interpreter [(we recommend installing pyenv)](https://github.com/pyenv/pyenv)
- [Docker](https://www.docker.com/)

### Upgrading GNU Make (macOS)
Although the macOS machines provide a working GNU `make` it is too old to support the capabilities within the DevOps utilities package, [makester](https://github.com/loum/makester). Instead, it is recommended to upgrade to the GNU make version provided by Homebrew. Detailed instructions can be found at https://formulae.brew.sh/formula/make . In short, to upgrade GNU make run:
```
brew install make
```
The `make` utility installed by Homebrew can be accessed by `gmake`. The https://formulae.brew.sh/formula/make notes suggest how you can update your local `PATH` to use `gmake` as `make`. Alternatively, alias `make`:
```
alias make=gmake
```
## Getting Started
### Creating the Local Environment
Get the code and change into the top level `git` project directory:
```
git clone git@github.com:loum/diffit.git && cd diffit
```
> **_NOTE:_** Run all commands from the top-level directory of the `git` repository.

For first-time setup, get the [Makester project](https://github.com/loum/makester.git):
```
git submodule update --init
```
Initialise the environment:
```
make init
```
#### Local Environment Maintenance
Keep [Makester project](https://github.com/loum/makester.git) up-to-date with:
```
git submodule update --remote --merge
```
## Help
There should be a `make` target to get most things done. Check the help for more information:
```
make help
```
## Running the Test Harness
Tests are good. We use [pytest](https://docs.pytest.org/en/6.2.x/). To run the tests:
```
make tests
```
## Docker Image Management
### Image Build
To build the Docker image against Apache Spark version `SPARK_VERSION` (defaults to `3.2.1`):
```
make build-image
```
You can target a specific Apache Spark release by setting `SPARK_VERSION`. For example:
```
SPARK_VERSION=3.0.3 make build image
```
The  tag naming policy follows `<spark-version>-<diffit-version>-<image-release-number>`.

### Validate the Image Build
A convenience target is provided for you to shake-out the new Docker image build against the `Dummy` schema:
```
make run
```
You can view the status of the job with:
```
make logs
```
On successful completion the `diffit` output will be written to `docker/files/data/out`

### Image Searches
Search for existing Docker image tags with command:
```
make search-image
```
### Docker Image Version Tag
```
make tag-version
```
Tagging convention used is:
```
<SBT_VERSION>-<RELEASE_VERSION>-<MAKESTER__RELEASE_NUMBER>
```
> **NOTE:** `RELEASE_VERSION` is calculated using [GitVersion](https://gitversion.net/) based on the repository's Git tag.

To see the current `RELEASE_VERSION`:
```
make version
```
```
### AssemblySemFileVer: 1.0.0
```
## `differ` Command: Invoke the Diffit Engine
`differ` is the command line interface that allows you to interact with the Diffit engine.
### Usage (development environment)
```
PYTHONPATH=src 3env/bin/python src/bin/differ --help
```
```
usage: diffit [-h] [-m DRIVER_MEMORY] {schema,row,analyse,columns,convert} ...

Diff-it Data Diff tool

positional arguments:
  {schema,row,analyse,columns,convert}
    schema              Diffit schema control
    row                 DataFrame row-level diff
    analyse             Diffit rows unique to source DataFrame
    columns             Report only the columns that are different
    convert             CSV to parquet

optional arguments:
  -h, --help            show this help message and exit
  -m DRIVER_MEMORY, --driver_memory DRIVER_MEMORY
                        Set Spark driver memory (default 2g)
```
### `differ schema` List Supported Schemas for CSV DataFrame Comparisons
List all native CSV schemas use the `differ` `schema` subcommand:
```
PYTHONPATH=src 3env/bin/python src/bin/differ schema --help
```
```
usage: differ schema [-h] {list} ...

positional arguments:
  {list}
    list      List supported schemas

options:
  -h, --help  show this help message and exit
```
### `differ row` DataFrame Comparator
> **_NOTE:_** Supported schema is required for CSV comparisons. For parquet data sources, set the `schema` switch to `parquet`.

The `differ` `row` reporter acts on two similarly constructed CSV (headers) or parquet (schema) output sources denoted as `left` and `right` and detects differences at the row level.

> **_NOTE:_** Another way to think about the `differ` `row` reporter is that identical rows from the `left` and `right` data sources are suppressed from the output.

`differ` `row` will produce a Diffit extract which is a Spark DataFrame in parquet format. The Diffit extract can then be further analysed using other `differ` sub-commands (see `differ analyse`) or with any other tooling that supports parquet.

A key characteristic of the Diffit extract is that it features a new column `diffit_ref`. This denotes the source reference that has caused the row level exception. Typically, this will be either `left` or `right`.
```
PYTHONPATH=src 3env/bin/python src/bin/differ row --help
```
```
usage: differ row [-h] [-o OUTPUT] [-d [DROP ...]] [-r RANGE] [-L LOWER] [-U UPPER] [-F]
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
A sample data set has been provided for the `Dummy` schema. This can be invoked as follows:
```
PYTHONPATH=src 3env/bin/python src/bin/differ row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
```
Use the local `pyspark` shell to view the results:
```
make pyspark
```
```
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
### `differ row` Report on Subset of DataFrame Columns
Sometimes it is preferable to run `differ` on a subset of DataFrame columns. To remove one or more unwanted DataFrame columns use the `drop` switch. For example, to drop `col02` from the local test sample:
```
PYTHONPATH=src 3env/bin/python src/bin/differ row --output /tmp/out --drop col02 -- Dummy docker/files/data/left docker/files/data/right
```
To view the results:
```
make pyspark
```
```
>>> df = spark.read.parquet('/tmp/out')
>>> df.show()
+-----+----------+
|col01|diffit_ref|
+-----+----------+
|    9|     right|
+-----+----------+
```
Multiple columns can be added to the `drop` switch separated by spaces. For example:
```
... --drop col01 col02 ... col<n> --
```
#### `differ row` Column Value Range Filtering
Filtering can be useful to limit `differ` to a subset of the original Spark SQL DataFrame. For example, we can limit the test data sources under `docker/files/data/left` to remove `col01` `3` and above as follows:
> **_NOTE:_**: Only `pyspark.sql.types.IntegerType` is currently supported.

```
PYTHONPATH=src 3env/bin/python src/bin/differ row --output /tmp/out --range col01 --lower 1 --upper 2 -- Dummy docker/files/data/left docker/files/data/right
```
To view the results:
```
make pyspark
```
```
>>> df = spark.read.parquet('/tmp/out')
>>> df.orderBy('col01').show()
+-----+-----------+----------+
|col01|      col02|diffit_ref|
+-----+-----------+----------+
|    2|col02_valXX|     right|
|    2|col02_val02|      left|
+-----+-----------+----------+
```

### `differ analyse` Reporting over the Diffit Extract
`differ analyse` supports the concept of a unique constraint key to target specific differences at the row level:
```
PYTHONPATH=src 3env/bin/python src/bin/differ analyse --help
```
```
usage: differ analyse [-h] [-R {left,right}] [-D] [-C] [-H HITS] [-r RANGE] [-L LOWER] [-U UPPER] [-F]
                      {distinct,altered} key diffit_out

positional arguments:
  {distinct,altered}    Report analysis type
  key                   column that acts as a unique constraint
  diffit_out            Path to Diffit output

options:
  -h, --help            show this help message and exit
  -R {left,right}, --diffit_ref {left,right}
                        target data source reference
  -D, --descending      Change output ordering to descending
  -C, --counts          Only output counts
  -H HITS, --hits HITS  Rows to display
  -r RANGE, --range RANGE
                        Column to target for range filter
  -L LOWER, --lower LOWER
                        Range filter lower bound (inclusive)
  -U UPPER, --upper UPPER
                        Range filter upper bound (inclusive)
  -F, --force-range     Force string-based range filter
```
#### `differ analyse distinct` Analyse Rows Unique to Each Source DataFrame
`differ analyse distinct` provides a way to report on Diffit extract rows that only appear in a specific target data source:
```
# Reset the Diffit extract.
PYTHONPATH=src 3env/bin/python src/bin/differ row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
PYTHONPATH=src 3env/bin/python src/bin/differ analyse distinct col01 /tmp/out
```
```
### Analysing distinct rows from "left" source DataFrame
+-----+-----+----------+
|col01|col02|diffit_ref|
+-----+-----+----------+
+-----+-----+----------+

### Analysing distinct rows from "right" source DataFrame
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|9    |col02_val09|right     |
+-----+-----------+----------+
```
> **_NOTE:_** The `key` acts as the `GROUP BY` predicate.

A Diffit extract can be limited with the `--diffit_ref` switch. For example, to only show distinct Diffit extract records from the `right` data source:
```
PYTHONPATH=src 3env/bin/python src/bin/differ analyse --diffit_ref right distinct col01 /tmp/out
```
```
# Output omitted for brevity.
...
### Analysing distinct rows from "right" source DataFrame
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|9    |col02_val09|right     |
+-----+-----------+----------+
```
The default number of rows returned is `20`. This can be adjusted with the `--hits` switch:
```
PYTHONPATH=src 3env/bin/python src/bin/differ analyse --diffit_ref right --hits 5 distinct col01 /tmp/out
```
#### `diffit analyse altered` Analyse Altered Rows from Each Source DataFrame
`differ analyse distinct` provides a way to report on Diffit extract rows that appear in both target data sources and flagged as different. For example, given a Diffit extract at `/tmp/out` that defines a schema column `col01` that can act as the unique constraint:
```
# Reset the Diffit extract.
PYTHONPATH=src 3env/bin/python src/bin/differ row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
PYTHONPATH=src 3env/bin/python src/bin/differ analyse altered col01 /tmp/out
```
```
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|2    |col02_val02|left      |
|2    |col02_valXX|right     |
+-----+-----------+----------+
```
> **_NOTE:_** The `key` acts as the as the unique constraint.

### `differ columns` Report on Columns that are Different
Some of the source DataFrames contain many columns. This makes identifying column differences difficult. The `differ columns` sub-command allows you to filter the Diffit extract rows and limit the output to only the column names and values that are different.
```
PYTHONPATH=src 3env/bin/python src/bin/differ columns --help
```
```
usage: differ columns [-h] key val diffit_out

positional arguments:
  key         column that acts as a unique constraint
  val         unique constraint column value to filter against
  diffit_out  Path to Diffit output

options:
  -h, --help  show this help message and exit
```
For example, given a Diffit extract at `/tmp/out` that features a schema column `col01` that can act as the unique constraint and a key column value of `2` as a filter:
```
PYTHONPATH=src 3env/bin/python src/bin/differ columns col01 2 /tmp/out
```
```
### col01|2: [
    {
        "col02": "col02_val02"
    }
]
```
> **_NOTE:_** Output is displayed as a JSON construct:

### `differ convert` Convert CSV to Parquet
Convert any supported schema to parquet format with a given compression (default `snappy`):
```
PYTHONPATH=src 3env/bin/python src/bin/differ convert --help
```
```
usage: differ convert [-h] [-z {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}] schema data_source output

positional arguments:
  schema                CSV schema to convert
  data_source           CSV source location
  output                Write parquet to path

options:
  -h, --help            show this help message and exit
  -z {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}, --compression {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}
                        Compression type
```
## Useful `make` Commands
### `make py`
Start the `python` interpreter in a virtual environment context. This will give you access to all of your PyPI package dependencies.

### `make pyspark`
Start the `pyspark` interpreter in virtual environment context. Handy if you want quick access to the Spark context.

## FAQs
**_Q. Why is the default make on macOS so old?_**
Apple seems to have an issue with licensing around GNU products: more specifically to the terms of the GPLv3 licence agreement. It is unlikely that Apple will provide current versions of utilities that are bound by the GPLv3 licensing constraints.

**_Q. Why do I get `WARNING: An illegal reflective access operation has occurred`?_**
Seems to be related to the JVM version being used. Java 8 will suppress the warning. To check available Java versions on your Mac try `/usr/libexec/java_home -V`. Then:
```
export JAVA_HOME=$(/usr/libexec/java_home -v <java_version>)
```

---
[top](#Diff-it-Data-Differ)
