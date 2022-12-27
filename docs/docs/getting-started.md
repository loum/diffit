# Getting started

Diffit can be used to validate PySpark transformation and aggregation logic at the Spark SQL
DataFrame level. The `diffit` package can be installed by adding the following to your project's
`setup.cfg`:
``` sh title="setup.cfg as an extras require"
[options.extras_require]
dev =
    diffit @ git+https://github.com/loum/diffit.git@<release_version>
```

!!! note
    See [Diffit releases](https://github.com/loum/diffit/releases) for latest version.
    

Editable installation can be achieved as follows:
``` sh title="pip editable install"
pip install -e .[dev]
```

## Unit Testing
The following example demonstrates how a complex aggregation with multiple Spark SQL DataFrame
source data sets can be validated against an expected state:
``` sh title="Unit testing a complex aggregation"
def test_aggregate(diagnostic_recursive: DataFrame,
                   diagnostic_static: DataFrame,
                   cross_strategy_positions: DataFrame,
                   portfolio_mapping: DataFrame,
                   diagnostic_reporting: DataFrame):
    """Diagnostic aggregate with static lookups.
    """
    # Given a Diagnostic extended data set
    # diagnostic_recursive

    # and a Diagnostic static lookup data set
    # diagnostic_static

    # and the Cross Strategy Positions data set
    # cross_strategy_positions

    # and the Portfolio Mapping data set
    # portfolio_mapping

    # when I aggregate to produce a consumable data set for Diagnostic
    received = pgc.liquidity.reporting.diagnostic.aggregate(diagnostic_recursive,
                                                            diagnostic_static,
                                                            cross_strategy_positions,
                                                            portfolio_mapping)

    # then the new DataFrame should align with the control Diagnostic consumed data set
    msg = 'Diagnostic consumed data set error'
    diffs: pyspark.sql.DataFrame = diffit.reporter.row_level(left=diagnostic_reporting, right=received)
    assert not [list(r) for r in diffs.collect()], msg
```

The `diffit` call of interest is `diffit.reporter.row_level`.Here a symmetric difference is performed
against the `left` Spark SQL DataFrame named `diagnostic_reporting` and the `received` DataFrame
produced by the aggregation output.

For brievity, the `assert` simply validates that the `diff` variable does not contain content. This
indicates that there is a problem and the test will fail. As `diff` is just a Spark SQL DataFrame
itself, you can manipulate `diff` further in your analysis to identify the problem.
