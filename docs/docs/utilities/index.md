# diffit Tooling

`diffit` provides a command line utility that you can use to invoke the Apache Spark-based
symmetric differential engine.

## Usage
```
usage: diffit [-h] [-m DRIVER_MEMORY] {schema,row,analyse,columns,convert} ...
```

```
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
