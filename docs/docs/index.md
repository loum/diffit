# A Spark SQL DataFrame Symmetric Difference Engine

The concept of [symmetric difference](https://en.wikipedia.org/wiki/Symmetric_difference) in
mathematics defines the set of elements that do not appear in either of the sets. It is leveraged
within the RDBMS community as a strategy for identifying differences across two tables. `diffit`
extends this capability to Apache Spark SQL DataFrames.

Diffit was created in response to challenges around validating massive data sets that use big data
file formats. However, Diffit is also flexible enough to be used within your unit test framework.
