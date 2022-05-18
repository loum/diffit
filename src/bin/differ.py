#!/usr/bin/env python

"""Diff-it executable.

"""
import os
import pathlib
import json
import argparse
from configparser import ConfigParser
from pyspark import (SparkContext, SparkConf)
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, col, desc

import diffit
import diffit.schema
import diffit.files
import diffit.reporter


DESCRIPTION = """Diff-it Data Diff tool"""

def main():
    """Script entry point.

    """
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('-m',
                        '--driver_memory',
                        default='2g',
                        help='Set Spark driver memory (default 2g)')

    # Add sub-command support.
    subparsers = parser.add_subparsers()

    # 'schema' subcommand.
    schema_parser = subparsers.add_parser('schema', help='Diffit schema control')
    schema_subparsers = schema_parser.add_subparsers(dest='schema')

    schema_list_parser = schema_subparsers.add_parser('list',
                                                      help='List supported schemas')

    schema_list_parser.set_defaults(func=schema_list)

    # 'row' subcommand.
    row_parser = subparsers.add_parser('row', help='DataFrame row-level diff')
    row_parser.add_argument('-o', '--output', help='Write results to path')
    row_parser.add_argument('-d', '--drop', nargs='*', help='Drop column from diffit engine')
    row_parser.add_argument('-r', '--range', help='Column to target for range filter')
    row_parser.add_argument('-L', '--lower', help='Range filter lower bound (inclusive)')
    row_parser.add_argument('-U', '--upper', help='Range filter upper bound (inclusive)')
    row_parser.add_argument('-F',
                            '--force-range',
                            action='store_true',
                            help='Force string-based range filter')
    row_parser.add_argument('schema', help='Report schema')
    row_parser.add_argument('left_data_source', help='"Left" DataFrame source location')
    row_parser.add_argument('right_data_source', help='"Right" DataFrame source location')
    row_parser.set_defaults(func=row)

    # 'analyse' subcommand.
    analyse_parser = subparsers.add_parser('analyse',
                                           help='Diffit rows unique to source DataFrame')

    analyse_parser.add_argument('-R',
                                '--diffit_ref',
                                choices=['left', 'right'],
                                help='target data source reference')
    analyse_parser.add_argument('-D',
                                '--descending',
                                action='store_true',
                                help='Change output ordering to descending')
    analyse_parser.add_argument('-C',
                                '--counts',
                                action='store_true',
                                help='Only output counts')
    analyse_parser.add_argument('-H', '--hits', default=20, help='Rows to display')
    analyse_parser.add_argument('type',
                                choices=['distinct', 'altered'],
                                help='Report analysis type')
    analyse_parser.add_argument('-r', '--range', help='Column to target for range filter')
    analyse_parser.add_argument('-L', '--lower', help='Range filter lower bound (inclusive)')
    analyse_parser.add_argument('-U', '--upper', help='Range filter upper bound (inclusive)')
    analyse_parser.add_argument('-F',
                                '--force-range',
                                action='store_true',
                                help='Force string-based range filter')
    analyse_parser.add_argument('key', help='column that acts as a unique constraint')
    analyse_parser.add_argument('diffit_out', help='Path to Diffit output')
    analyse_parser.set_defaults(func=analyse)

    # 'columns' subcommand.
    columns_parser = subparsers.add_parser('columns',
                                           help='Report only the columns that are different')
    columns_parser.add_argument('key', help='column that acts as a unique constraint')
    columns_parser.add_argument('val', help='unique constraint column value to filter against')
    columns_parser.add_argument('diffit_out', help='Path to Diffit output')
    columns_parser.set_defaults(func=columns)

    # 'convert' subcommand.
    convert_parser = subparsers.add_parser('convert', help='CSV to parquet')
    convert_parser.add_argument('schema', help='CSV schema to convert')
    convert_parser.add_argument('data_source', help='CSV source location')
    convert_parser.add_argument('output', help='Write parquet to path')
    convert_parser.add_argument('-z',
                                '--compression',
                                choices=[
                                    'brotli',
                                    'uncompressed',
                                    'lz4',
                                    'gzip',
                                    'lzo',
                                    'snappy',
                                    'none',
                                    'zstd'
                                ],
                                default='snappy',
                                help='Compression type')
    convert_parser.set_defaults(func=convert)

    # Prepare the argument list.
    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_help()
        parser.exit()
    func(args)


def schema_list(args): # pylint: disable=unused-argument
    """Diffit schema list.

    """
    for index, schema in enumerate(sorted(diffit.schema.names()), start=1):
        print(f'{index}. {schema[0][0]}')


def row(args):
    """Report against given schema.

    """
    spark = SparkSession(SparkContext(conf=spark_conf(driver_memory=args.driver_memory)))

    if args.schema == 'parquet':
        left = diffit.files.spark_parquet_reader(spark, args.left_data_source)
        right = diffit.files.spark_parquet_reader(spark, args.right_data_source)
    else:
        schema = diffit.schema.get(args.schema)
        left = diffit.files.spark_csv_reader(spark, schema, args.left_data_source)
        right = diffit.files.spark_csv_reader(spark, schema, args.right_data_source)

    range_filter = {
        'column': args.range,
        'lower': args.lower,
        'upper': args.upper,
        'force': args.force_range,
    }

    if args.output:
        diffit.reporter.row_level(left, right, args.drop, range_filter)\
            .write.mode('overwrite')\
            .parquet(args.output)
    else:
        diffit.reporter.row_level(left, right).show(truncate=False)


def analyse(args):
    """Extract Diffit output rows that only present from the source DataFrame.

    """
    spark = SparkSession(SparkContext(conf=spark_conf(driver_memory=args.driver_memory)))

    diffit_df = diffit.files.spark_parquet_reader(spark, args.diffit_out)

    order = asc
    if args.descending:
        order = desc

    range_filter = {
        'column': args.range,
        'lower': args.lower,
        'upper': args.upper,
        'force': args.force_range,
    }

    if args.type == 'distinct':
        for ref in ['left', 'right']:
            if args.diffit_ref is not None and ref != args.diffit_ref:
                continue

            print(f'### Analysing distinct rows from "{ref}" source DataFrame')
            cmd = diffit.reporter.distinct_rows(diffit_df, args.key, ref)\
                .sort(order(col(args.key)))
            if args.counts:
                print(cmd.count())
            else:
                cmd.show(int(args.hits), truncate=False)
    elif args.type == 'altered':
        cmd = diffit.reporter.altered_rows(diffit_df, args.key, range_filter)\
            .sort(order(col(args.key)), col('diffit_ref').asc())
        if args.counts:
            print(cmd.count())
        else:
            cmd.show(int(args.hits), truncate=False)


def columns(args):
    """Show different columns.

    """
    spark = SparkSession(SparkContext(conf=spark_conf(driver_memory=args.driver_memory)))

    diffit_df = diffit.files.spark_parquet_reader(spark, args.diffit_out)

    result = diffit.reporter.altered_rows_column_diffs(diffit_df, args.key, args.val)
    print(f'### {args.key}|{args.val}: '
          f'{json.dumps(list(result), indent=4, sort_keys=True, default=str)}')


def convert(args):
    """Convert CSV to parquet.

    """
    spark = SparkSession(SparkContext(conf=spark_conf(driver_memory=args.driver_memory)))

    diffit.files.spark_csv_reader(spark, diffit.schema.get(args.schema), args.data_source)\
        .repartition(8)\
        .write.mode('overwrite')\
        .option('compression', args.compression)\
        .parquet(args.output)


def spark_conf(driver_memory: str = '2g') -> SparkConf:
    """Generic Spark configuration settings.

    """
    conf = SparkConf()
    conf.setAppName(DESCRIPTION)
    conf.set('spark.driver.memory', driver_memory)
    conf.set('spark.hadoop.fs.s3a.endpoint', 's3.ap-southeast-2.amazonaws.com')
    conf.set('spark.hadoop.fs.s3a.aws.experimental.input.fadvise', 'random')
    aws_path = os.path.join(pathlib.Path.home(), '.aws', 'credentials')
    if os.path.exists(aws_path):
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
        with open(aws_path, encoding='utf-8') as _fh:
            aws_config = ConfigParser()
            aws_config.read_file(_fh)
            conf.set('spark.hadoop.fs.s3a.access.key', aws_config.get('default',
                                                                      'aws_access_key_id'))
            conf.set('spark.hadoop.fs.s3a.secret.key', aws_config.get('default',
                                                                      'aws_secret_access_key'))
            conf.set('spark.hadoop.fs.s3a.session.token', aws_config.get('default',
                                                                         'aws_session_token'))
    else:
        kms_key_arn = os.environ.get('KMS_KEY_ARN')
        if kms_key_arn:
            conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider')
            conf.set('spark.hadoop.fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
            conf.set('spark.hadoop.fs.s3a.server-side-encryption.key', kms_key_arn)

    return conf


if __name__ == '__main__':
    main()
