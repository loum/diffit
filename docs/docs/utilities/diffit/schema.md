# diffit schema

List all native CSV schemas use the `diffit schema` subcommand.

## Usage
``` sh
venv/bin/diffit schema --help
```

``` sh title="diffit schema help"
usage: diffit schema [-h] {list} ...

positional arguments:
  {list}
    list      List supported schemas

options:
  -h, --help  show this help message and exit
```

## Example
`diffit` provides a sample CSV schema, `Dummy`, that can be used for evaluation purposes:
```
venv/bin/diffit schema list
```

```
1. Dummy
```
