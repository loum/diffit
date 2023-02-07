"""Diff-it executable.

"""
from typing import Optional, cast
import json
import argparse

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import asc, col, desc

import diffit
import diffit.datasources.spark
import diffit.reporter
import diffit.schema


DESCRIPTION = """Diff-it Data Diff tool"""


def main() -> None:
    """Script entry point."""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "-m",
        "--driver_memory",
        default="2g",
        help="Set Spark driver memory (default 2g)",
    )

    # Add sub-command support.
    subparsers = parser.add_subparsers()

    # 'row' subcommand.
    row_parser = subparsers.add_parser("row", help="DataFrame row-level diff")
    row_parser.add_argument("-o", "--output", help="Write results to path")
    row_parser.add_argument(
        "-d", "--drop", action="append", help="Drop column from diffit engine"
    )
    row_parser.add_argument("-r", "--range", help="Column to target for range filter")
    row_parser.add_argument(
        "-L", "--lower", help="Range filter lower bound (inclusive)"
    )
    row_parser.add_argument(
        "-U", "--upper", help="Range filter upper bound (inclusive)"
    )
    row_parser.add_argument(
        "-F",
        "--force-range",
        action="store_true",
        help="Force string-based range filter",
    )

    row_subparser = row_parser.add_subparsers(title="sub-commands", dest="subcommand")

    # 'row csv' subcommand.
    row_parser_csv = row_subparser.add_parser("csv", help="CSV row-level")
    row_parser_csv.add_argument(
        "-s", "--csv-separator", help="CSV separator", default=","
    )
    row_parser_csv.add_argument(
        "-E", "--csv-header", help="CSV contains header", action="store_true"
    )
    row_parser_csv.add_argument("schema", help="Path to CSV schema in JSON format")
    row_parser_csv.add_argument("left_data_source", help='"Left" CSV source location')
    row_parser_csv.add_argument("right_data_source", help='"Right" CSV source location')
    row_parser_csv.set_defaults(func=row_csv)

    # 'row parquet' subcommand.
    row_parser_parquet = row_subparser.add_parser("parquet", help="Parquet row-level")
    row_parser_parquet.add_argument(
        "left_data_source", help='"Left" Parquet source location'
    )
    row_parser_parquet.add_argument(
        "right_data_source", help='"Right" Parquet source location'
    )
    row_parser_parquet.set_defaults(func=row_parquet)

    # 'analyse' subcommand.
    analyse_parser = subparsers.add_parser(
        "analyse", help="Diffit rows unique to source DataFrame"
    )

    analyse_parser.add_argument(
        "-R",
        "--diffit_ref",
        choices=["left", "right"],
        help="target data source reference",
    )
    analyse_parser.add_argument(
        "-D",
        "--descending",
        action="store_true",
        help="Change output ordering to descending",
    )
    analyse_parser.add_argument(
        "-C", "--counts", action="store_true", help="Only output counts"
    )
    analyse_parser.add_argument("-H", "--hits", default=20, help="Rows to display")
    analyse_parser.add_argument(
        "type", choices=["distinct", "altered"], help="Report analysis type"
    )
    analyse_parser.add_argument(
        "-r", "--range", help="Column to target for range filter"
    )
    analyse_parser.add_argument(
        "-L", "--lower", help="Range filter lower bound (inclusive)"
    )
    analyse_parser.add_argument(
        "-U", "--upper", help="Range filter upper bound (inclusive)"
    )
    analyse_parser.add_argument(
        "-F",
        "--force-range",
        action="store_true",
        help="Force string-based range filter",
    )
    analyse_parser.add_argument("key", help="column that acts as a unique constraint")
    analyse_parser.add_argument("diffit_out", help="Path to Diffit output")
    analyse_parser.set_defaults(func=analyse)

    # 'columns' subcommand.
    columns_parser = subparsers.add_parser(
        "columns", help="Report only the columns that are different"
    )
    columns_parser.add_argument("key", help="column that acts as a unique constraint")
    columns_parser.add_argument(
        "val", help="unique constraint column value to filter against"
    )
    columns_parser.add_argument("diffit_out", help="Path to Diffit output")
    columns_parser.set_defaults(func=columns)

    # 'convert' subcommand.
    convert_parser = subparsers.add_parser("convert", help="CSV to parquet")
    convert_parser.add_argument("schema", help="CSV schema to convert")
    convert_parser.add_argument("data_source", help="CSV source location")
    convert_parser.add_argument("output", help="Write parquet to path")
    convert_parser.add_argument(
        "-z",
        "--compression",
        choices=[
            "brotli",
            "uncompressed",
            "lz4",
            "gzip",
            "lzo",
            "snappy",
            "none",
            "zstd",
        ],
        default="snappy",
        help="Compression type",
    )
    convert_parser.set_defaults(func=convert)

    # Prepare the argument list.
    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_help()
        parser.exit()

    conf = SparkConf()
    conf.set("spark.driver.memory", args.driver_memory)
    args.conf = conf

    func(args)


def row_csv(args: argparse.Namespace) -> None:
    """Row-level CSV pre-processing."""
    schema = diffit.schema.interpret_schema(args.schema)

    if schema is not None:
        spark = diffit.datasources.spark.spark_session(conf=args.conf)
        left = diffit.datasources.spark.csv_reader(
            spark,
            schema,
            args.left_data_source,
            delimiter=args.csv_separator,
            header=args.csv_header,
        )
        right = diffit.datasources.spark.csv_reader(
            spark,
            schema,
            args.right_data_source,
            delimiter=args.csv_separator,
            header=args.csv_header,
        )
        row_report(args, left, right)


def row_parquet(args: argparse.Namespace, left: DataFrame, right: DataFrame) -> None:
    """Row-level parquet pre-processing."""
    spark = diffit.datasources.spark.spark_session(conf=args.conf)
    left = diffit.datasources.spark.parquet_reader(spark, args.left_data_source)
    right = diffit.datasources.spark.parquet_reader(spark, args.right_data_source)
    row_report(args, left, right)


def row_report(args: argparse.Namespace, left: DataFrame, right: DataFrame) -> None:
    """Row-level reporter."""
    range_filter = {
        "column": args.range,
        "lower": args.lower,
        "upper": args.upper,
        "force": args.force_range,
    }

    reporter = diffit.reporter.row_level(left, right, args.drop, range_filter)
    if args.output:
        cast(DataFrame, reporter).write.mode("overwrite").parquet(args.output)
    else:
        cast(DataFrame, reporter).show(truncate=False)


def analyse(args: argparse.Namespace) -> None:
    """Extract Diffit output rows that only present from the source DataFrame."""
    spark = diffit.datasources.spark.spark_session(conf=args.conf)

    diffit_df = diffit.datasources.spark.parquet_reader(spark, args.diffit_out)

    order = asc
    if args.descending:
        order = desc

    range_filter = {
        "column": args.range,
        "lower": args.lower,
        "upper": args.upper,
        "force": args.force_range,
    }

    if args.type == "distinct":
        for ref in ["left", "right"]:
            if args.diffit_ref is not None and ref != args.diffit_ref:
                continue

            print(f'### Analysing distinct rows from "{ref}" source DataFrame')
            cmd = diffit.reporter.distinct_rows(diffit_df, args.key, ref).sort(
                order(col(args.key))
            )
            if args.counts:
                print(cmd.count())
            else:
                cmd.show(int(args.hits), truncate=False)
    elif args.type == "altered":
        cmd = diffit.reporter.altered_rows(diffit_df, args.key, range_filter).sort(
            order(col(args.key)), col("diffit_ref").asc()
        )
        if args.counts:
            print(cmd.count())
        else:
            cmd.show(int(args.hits), truncate=False)


def columns(args: argparse.Namespace) -> None:
    """Show different columns."""
    spark = diffit.datasources.spark.spark_session(conf=args.conf)

    diffit_df = diffit.datasources.spark.parquet_reader(spark, args.diffit_out)

    result = diffit.reporter.altered_rows_column_diffs(diffit_df, args.key, args.val)
    print(
        f"### {args.key}|{args.val}: "
        f"{json.dumps(list(result), indent=4, sort_keys=True, default=str)}"
    )


def convert(args: argparse.Namespace) -> None:
    """Convert CSV to parquet."""
    spark = diffit.datasources.spark.spark_session(conf=args.conf)

    schema: Optional[StructType] = diffit.schema.interpret_schema(args.schema)
    if schema is not None:
        diffit.datasources.spark.csv_reader(
            spark, schema, args.data_source
        ).repartition(8).write.mode("overwrite").option(
            "compression", args.compression
        ).parquet(
            args.output
        )
