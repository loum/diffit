# diffit columns

Report on Spark DataFrame column/value pair differences.

Diffit extracts can be large, based on the number of exceptions detected. `diffit columns` allows
you to report on a specific key/value pairing for targeted analysis.

Output is displayed as a JSON construct.

## Usage
``` sh
venv/bin/diffit columns --help
```

``` sh
usage: diffit columns [-h] key val diffit_out

positional arguments:
  key         column that acts as a unique constraint
  val         unique constraint column value to filter against
  diffit_out  Path to Diffit output

options:
  -h, --help  show this help message and exit
```

## Example
``` sh title="Reset the Diffit extract"
venv/bin/diffit row --output /tmp/out csv --csv-separator ';' /tmp/Dummy.json docker/files/data/left docker/files/data/right
```

The Diffit extract at `/tmp/out` features:

- A schema column `col01` acting as the unique constraint
- A key column value of `2` as a filter

``` sh title="diffit columns filter for key:value pair col01:2"
venv/bin/diffit columns col01 2 /tmp/out
```

``` sh title="Result"
### col01|2: [
    {
        "col02": "col02_val02"
    }
]
```
