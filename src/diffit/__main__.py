"""Diffit CLI.

"""
from dataclasses import dataclass
from enum import Enum
from typing import Callable, ClassVar, Dict, Iterable, List, Optional, Text
import json

from logga import log, suppress_logging
from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import typer

from diffit.reporter.rangefilter import RangeFilter
import diffit
import diffit.schema
import diffit.datastore.spark as diffit_spark


app = typer.Typer(
    add_completion=False,
    help="Diff-it Data Diff tool.",
)

row_app = typer.Typer(
    add_completion=False,
    help="Filter out identical rows from the left and right data sources.",
)
app.add_typer(row_app, name="row")

analyse_app = typer.Typer(
    add_completion=False,
    help="List rows that are unique to the nominated source DataFrame.",
)
app.add_typer(analyse_app, name="analyse")

columns_app = typer.Typer(
    add_completion=False,
    help="Display the columns that are different.",
)
app.add_typer(columns_app, name="columns")

convert_app = typer.Typer(
    add_completion=False,
    help="Convert CSV to Apache Parquet.",
)
app.add_typer(convert_app, name="convert")


@dataclass
class Common:
    """Common arguments at the command level."""

    quiet: bool
    driver_memory: Text

    def spark_session(
        self, name: Text = diffit.__app_name__, conf: Optional[SparkConf] = None
    ) -> SparkSession:
        """Configured Spark session getter.

        Parameters:
            name: The SparkSession context to build the configuration.

        Returns:
            The correct SparkSession to interface with the data interchange.

        """
        if conf is None:
            conf = SparkConf()

        conf.set("spark.driver.memory", self.driver_memory)
        return diffit_spark.spark_session(
            app_name=name, conf=diffit_spark.spark_conf(app_name=name, conf=conf)
        )


@app.callback()
def common(
    ctx: typer.Context,
    quiet: bool = typer.Option(
        False, "--quiet", help='Disable logs to screen (to log level "ERROR").'
    ),
    driver_memory: Text = typer.Option(
        "1g",
        "--driver-memory",
        "-M",
        help="Set Spark driver memory.",
    ),
) -> None:
    """Define the common arguments."""
    ctx.obj = Common(quiet, driver_memory)

    if ctx.obj.quiet:
        suppress_logging()


# Row options.
option_add = typer.Option(
    None,
    "--add",
    "-a",
    help="Add column to the diffit engine",
    show_default=False,
)
option_drop = typer.Option(
    None,
    "--drop",
    "-d",
    help="Drop column from diffit engine",
    show_default=False,
)
option_range_column = typer.Option(
    None,
    "--range-column",
    "-r",
    help="Column to target for range filter",
    show_default=False,
)
option_lower = typer.Option(
    None,
    "--lower",
    "-L",
    help="Range filter lower bound (inclusive)",
    show_default=False,
)
option_upper = typer.Option(
    None,
    "--upper",
    "-U",
    help="Range filter upper bound (inclusive)",
    show_default=False,
)
option_force_range = typer.Option(
    False,
    "--force-range",
    "-F",
    help="Force (cast) string-based range column filter",
)
option_csv_separator = typer.Option(
    ",",
    "--csv-separator",
    "-s",
    help="CSV separator",
)
option_csv_header = typer.Option(
    False,
    "--csv-header",
    "-E",
    help="CSV contains header",
)
option_json_schema = typer.Option(
    ...,
    "--json-schema",
    "-J",
    help="Path to CSV schema in JSON format",
    show_default=False,
)

# File location options.
arg_parquet: Callable[[Text], Text] = lambda x: typer.Argument(
    ..., help=f"Path to Spark Parquet: {x}", show_default=False
)
option_parquet: Callable[[Text], Text] = lambda x: typer.Option(
    None, help=f"Path to Spark Parquet: {x}", show_default=False
)
option_left_data_source = typer.Option(
    ..., "--left", "-l", help="Path to left data source", show_default=False
)
option_right_data_source = typer.Option(
    ..., "--right", "-r", help="Path to right data source", show_default=False
)


@row_app.command(name="csv")
def row_csv(  # pylint: disable=too-many-arguments,too-many-locals
    ctx: typer.Context,
    add: Optional[List[Text]] = option_add,
    drop: Optional[List[Text]] = option_drop,
    range_column: Optional[Text] = option_range_column,
    lower: Optional[int] = option_lower,
    upper: Optional[int] = option_upper,
    force_range: bool = option_force_range,
    csv_separator: Text = option_csv_separator,
    csv_header: bool = option_csv_header,
    json_schema: Text = option_json_schema,
    left_data_source: Text = option_left_data_source,
    right_data_source: Text = option_right_data_source,
    parquet_path: Text = option_parquet("output"),
) -> None:
    """Spark DataFrame row-level diff from CSV source data."""

    range_filter = None
    if range_column is not None:
        range_filter = RangeFilter(
            column=range_column,
            lower=lower,
            upper=upper,
            force_range=force_range,
        )

    schema: Optional[StructType] = diffit.schema.interpret_schema(json_schema)

    if range_column is not None and not range_filter.thresholds_are_valid or not schema:
        raise typer.Abort()

    spark = ctx.obj.spark_session()
    left: DataFrame = diffit.datastore.spark.csv_reader(
        spark,
        schema,
        left_data_source,
        delimiter=csv_separator,
        header=csv_header,
    )
    right: DataFrame = diffit.datastore.spark.csv_reader(
        spark,
        schema,
        right_data_source,
        delimiter=csv_separator,
        header=csv_header,
    )

    diffit_spark.parquet_writer(left, "docker/files/parquet/left")
    diffit_spark.parquet_writer(right, "docker/files/parquet/right")

    reporter: DataFrame = diffit.reporter.row_level(
        left, right, columns_to_add=add, columns_to_drop=drop, range_filter=range_filter
    )
    if parquet_path:
        diffit_spark.parquet_writer(reporter, parquet_path)
    else:
        reporter.show(truncate=False)


@row_app.command(name="parquet")
def row_parquet(  # pylint: disable=too-many-arguments
    ctx: typer.Context,
    add: Optional[List[str]] = option_add,
    drop: Optional[List[str]] = option_drop,
    range_column: Optional[Text] = option_range_column,
    lower: Optional[int] = option_lower,
    upper: Optional[int] = option_upper,
    force_range: bool = option_force_range,
    left_data_source: Text = option_left_data_source,
    right_data_source: Text = option_right_data_source,
    parquet_path: Text = option_parquet("output"),
) -> None:
    """Spark DataFrame row-level diff from Spark Parquet source data."""

    range_filter = None
    if range_column is not None:
        range_filter = RangeFilter(
            column=range_column,
            lower=lower,
            upper=upper,
            force_range=force_range,
        )

    if range_column is not None and not range_filter.thresholds_are_valid:
        raise typer.Abort()

    spark = ctx.obj.spark_session()
    left: DataFrame = diffit.datastore.spark.parquet_reader(spark, left_data_source)
    right: DataFrame = diffit.datastore.spark.parquet_reader(spark, right_data_source)

    reporter: DataFrame = diffit.reporter.row_level(
        left, right, columns_to_add=add, columns_to_drop=drop, range_filter=range_filter
    )
    if parquet_path:
        diffit_spark.parquet_writer(reporter, parquet_path)
    else:
        reporter.show(truncate=False)


@dataclass
class AnalyseOrientation(str, Enum):
    """Diffit DataFrame analyse orientation."""

    LEFT: ClassVar[Text] = "left"
    RIGHT: ClassVar[Text] = "right"


option_orientation = typer.Option(
    None,
    "--orientation",
    "-O",
    case_sensitive=False,
    help='Limit analysis orientation to either "left" or "right"',
    show_default=False,
)
option_key = typer.Option(
    ..., "--key", "-k", help="Analysis column to act as a unique constraint"
)
option_desc = typer.Option(
    False, "--descending", "-D", help="Change output ordering to descending"
)
option_counts_only = typer.Option(
    False, "--counts-only", "-C", help="Only output counts"
)
option_hits = typer.Option(20, "--hits", "-H", help="Rows to display")


@analyse_app.command("distinct")
def analyse_distinct(  # pylint: disable=too-many-arguments
    ctx: typer.Context,
    orientation: AnalyseOrientation = option_orientation,
    key: Text = option_key,
    descending: bool = option_desc,
    counts_only: bool = option_counts_only,
    hits: int = option_hits,
    parquet_path: Text = arg_parquet("input"),
) -> None:
    """Spark DataFrame list rows source data."""

    spark = ctx.obj.spark_session()
    analysis: DataFrame = diffit.datastore.spark.parquet_reader(spark, parquet_path)

    order: Callable[..., Column] = F.asc
    if descending:
        order = F.desc

    for ref in ["left", "right"]:
        if orientation is not None and ref != orientation:
            continue

        log.info('Analysing distinct rows from "%s" source DataFrame', ref)
        distinct: DataFrame = diffit.reporter.distinct_rows(
            analysis, key, diffit_ref=ref
        ).sort(order(F.col(key)))

        if counts_only:
            log.info("Distinct rows analysis count: %s", distinct.count())
        else:
            distinct.show(int(hits), truncate=False)


@analyse_app.command("altered")
def analyse_altered(  # pylint: disable=too-many-arguments,too-many-locals
    ctx: typer.Context,
    key: Text = option_key,
    range_column: Optional[Text] = option_range_column,
    lower: Optional[int] = option_lower,
    upper: Optional[int] = option_upper,
    force_range: bool = option_force_range,
    descending: bool = option_desc,
    counts_only: bool = option_counts_only,
    hits: int = option_hits,
    parquet_path: Text = arg_parquet("input"),
) -> None:
    """Spark DataFrame list rows source data."""

    range_filter = None
    if range_column is not None:
        range_filter = RangeFilter(
            column=range_column,
            lower=lower,
            upper=upper,
            force_range=force_range,
        )

    if range_column is not None and not range_filter.thresholds_are_valid:
        raise typer.Abort()

    spark = ctx.obj.spark_session()
    analysis: DataFrame = diffit.datastore.spark.parquet_reader(spark, parquet_path)

    order: Callable[..., Column] = F.asc
    if descending:
        order = F.desc

    altered: DataFrame = diffit.reporter.altered_rows(
        analysis, key, range_filter=range_filter
    ).sort(order(F.col(key)), F.col("diffit_ref").asc())

    if counts_only:
        total_altered: int = altered.count()
        if total_altered:
            total_altered = int(total_altered / 2)
        log.info("Altered rows analysis count: %s", total_altered)
    else:
        altered.show(int(hits), truncate=False)


@columns_app.command("diff")
def columns_diff(
    ctx: typer.Context,
    key: Text = option_key,
    value: Text = typer.Option(
        ...,
        "--value",
        "-V",
        help="Unique constraint column value to filter against",
        show_default=False,
    ),
    parquet_path: Text = arg_parquet("input"),
) -> None:
    """Analyse altered column name differences."""

    spark = ctx.obj.spark_session()
    analysis: DataFrame = diffit.datastore.spark.parquet_reader(spark, parquet_path)

    columns: Iterable[Dict] = diffit.reporter.altered_rows_column_diffs(
        analysis, key, value
    )
    log.info(
        'Analyse diffs for column "%s" and value "%s" result:\n%s',
        key,
        value,
        json.dumps(list(columns), indent=4, sort_keys=True, default=str),
    )


@dataclass
class CompressionTypes(str, Enum):
    """Apache Parquet compression types."""

    BROTLI: ClassVar[Text] = "brotli"
    GZIP: ClassVar[Text] = "gzip"
    LZ4: ClassVar[Text] = "lz4"
    LZO: ClassVar[Text] = "lzo"
    NONE: ClassVar[Text] = "none"
    SNAPPY: ClassVar[Text] = "snappy"
    UNCOMPRESSED: ClassVar[Text] = "uncompressed"
    ZSTD: ClassVar[Text] = "zstd"


@convert_app.command(name="csv")
def convert_csv(  # pylint: disable=too-many-arguments
    ctx: typer.Context,
    csv_separator: Text = option_csv_separator,
    csv_header: bool = option_csv_header,
    json_schema: Text = option_json_schema,
    csv_data_source: Text = typer.Option(
        ..., "--csv-data", "-C", help="Path to CSV data source", show_default=False
    ),
    compression_type: CompressionTypes = typer.Option(
        CompressionTypes.SNAPPY,
        "--compression-type",
        "-Z",
        help="Compression type",
        case_sensitive=False,
    ),
    num_partitions: int = typer.Option(
        8,
        "--num-partitions",
        "-N",
        help="Number of partitions",
    ),
    parquet_path: Text = arg_parquet("output"),
) -> None:
    """Convert CSV to Apache Parquet."""

    spark = ctx.obj.spark_session()

    schema: Optional[StructType] = diffit.schema.interpret_schema(json_schema)

    if not schema:
        raise typer.Abort()

    diffit.datastore.spark.csv_reader(
        spark,
        schema,
        csv_data_source,
        delimiter=csv_separator,
        header=csv_header,
    ).repartition(num_partitions).write.mode(diffit_spark.Mode.OVERWRITE).option(
        "compression", compression_type.value
    ).parquet(
        parquet_path
    )


def main() -> None:
    """Script entry point."""
    app()


if __name__ == "__main__":
    main()
