# diffit convert

Convert as CSV data source with schema file to
[Spark Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
to with a given compression (default `snappy`)

## Usage
``` sh title="diffit convert help"
venv/bin/diffit convert --help
```

``` sh
usage: diffit convert [-h] [-z {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}] schema data_source output

positional arguments:
  schema                CSV schema to convert
  data_source           CSV source location
  output                Write parquet to path

options:
  -h, --help            show this help message and exit
  -z {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}, --compression {brotli,uncompressed,lz4,gzip,lzo,snappy,none,zstd}
                        Compression type
```
