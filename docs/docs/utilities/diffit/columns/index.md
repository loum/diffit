# Usage

Display the column names where the Spark DataFrame column/value pair show differences.

`diffit` engine analysis for altered columns will display the entire row. For wide row, this could
become unweildy to manage. `diffit columns` allows you to only list on the key/value pairing column
names that allow a targeted analysis.

Output is displayed as a JSON construct.

``` sh
venv/bin/diffit columns --help
```

``` sh title="diffit columns usage message."
 Usage: diffit columns [OPTIONS] COMMAND [ARGS]...

 Display the columns that are different

╭─ Options ────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                          │
╰──────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────╮
│ diff         Analyse altered column name differences.                │
╰──────────────────────────────────────────────────────────────────────╯
```
