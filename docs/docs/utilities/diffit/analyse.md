# diffit analyse

`diffit analyse` supports the concept of a unique constraint key to target differences at the row level.

## Usage
``` sh
venv/bin/diffit analyse --help
```

``` sh
usage: diffit analyse [-h] [-R {left,right}] [-D] [-C] [-H HITS] [-r RANGE] [-L LOWER] [-U UPPER] [-F]
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

## `diffit analyse distinct`
`diffit analyse distinct` provides a way to report on rows from a Diffit extract that
only appear in a specific target data source.

### Example: Analyse Rows Unique to Each Spark DataFrame
``` sh title="Reset the Diffit extract"
venv/bin/diffit row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
```

``` sh title="The key setting, col01, acts as the GROUP BY predicate"
venv/bin/diffit analyse distinct col01 /tmp/out
```

``` sh title="Analysing distinct rows from left source DataFrame"
+-----+-----+----------+
|col01|col02|diffit_ref|
+-----+-----+----------+
+-----+-----+----------+
```

``` sh title="Analysing distinct rows from right source DataFrame"
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|9    |col02_val09|right     |
+-----+-----------+----------+
```

A Diffit extract can be limited with the `--diffit_ref` switch. For example, to only show
distinct Diffit extract records from the `right` data source:
``` sh title="Analysing distinct rows from right source Spark DataFrame"
venv/bin/diffit analyse --diffit_ref right distinct col01 /tmp/out
```

``` sh title="Result"
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|9    |col02_val09|right     |
+-----+-----------+----------+
```

The default number of rows returned is `20`. This can be adjusted with the `--hits` switch:
``` sh
venv/bin/diffit analyse --diffit_ref right --hits 5 distinct col01 /tmp/out
```

## `diffit analyse altered`
`diffit analyse distinct` provides a way to report on Diffit extract rows that appear in both target
data sources and flagged as being different.

### Example
Given a Diffit extract at `/tmp/out` that defines a schema column `col01` that can act as the unique constraint:

``` sh title="Reset the Diffit extract"
venv/bin/diffit row --output /tmp/out Dummy docker/files/data/left docker/files/data/right
```

``` sh
venv/bin/diffit analyse altered col01 /tmp/out
```

``` sh title="Result"
+-----+-----------+----------+
|col01|col02      |diffit_ref|
+-----+-----------+----------+
|2    |col02_val02|left      |
|2    |col02_valXX|right     |
+-----+-----------+----------+
```
