# Usage

`diffit analyse` works over the `diffit` engine output that is created with
[diffit row](../row/index.md).

`diffit analyse` supports the concept of a unique constraint key to target differences at the row level.

``` sh
venv/bin/diffit analyse --help
```

``` sh title="diffit analyse usage message."
 Usage: diffit analyse [OPTIONS] COMMAND [ARGS]...

 List rows that are unique to the nominated source DataFrame.

╭─ Options ────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                          │
╰──────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────╮
│ distinct              Spark DataFrame list rows source data.         │
╰──────────────────────────────────────────────────────────────────────╯
```
