# Getting started

Diffit can be used to validate PySpark transformation and aggregation logic at the Spark SQL
DataFrame level. The `diffit` package can be installed by adding the following to your project's
`setup.cfg`:
``` sh title="setup.cfg as an extras require."
[options.extras_require]
dev =
    diffit @ git+https://github.com/loum/diffit.git@<release_version>
```

!!! note
    See [Diffit releases](https://github.com/loum/diffit/releases) for latest version.
    
Editable installation can be achieved as follows:

``` sh title="pip editable install."
pip install -e .[dev]
```

## Unit Testing

Here is a trivial use case that checks that two Spark SQL DataFrame are the same:

``` sh title="A trivial Diffit check."
def test_symmetric_difference_check_ok(spark: SparkSession, dummy: DataFrame):
    """Symmetric difference check: no error."""
    # Given a Dummy Spark DataFrame
    # dummy

    # when I compare it against itself at the DataFrame level
    diffs: DataFrame = diffit.reporter.row_level(left=dummy, right=dummy)

    # then the Diffit integration check should not detect an error
    msg = "Symmetric difference SHOULD NOT BE detected."
    assert not [list(r) for r in diffs.collect()], msg
```

The `diffit` call of interest is `diffit.reporter.row_level`. Here, a symmetric difference
is performed against the `left` Spark SQL DataFrame named `dummy` onto itself.

For brievity, the `assert` simply validates that the `diff` variable does not contain content.
If `diffit` produces output, then this indicates that there is a problem and the test
will fail. As the `diff` variable is just a Spark SQL DataFrame itself, you can manipulate
`diff` content to further investigate the error.
