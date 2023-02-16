"""SparkSession as a data source.

"""
from collections import deque
from configparser import ConfigParser
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Optional, Text
import os
import pathlib

from logga import log
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

import diffit


@dataclass
class Mode(Text, Enum):
    """Spark DataFrame write modes."""

    APPEND: ClassVar[Text] = "append"
    ERROR: ClassVar[Text] = "error"
    IGNORE: ClassVar[Text] = "ignore"
    OVERWRITE: ClassVar[Text] = "overwrite"


def spark_conf(app_name: Text, conf: Optional[SparkConf] = None) -> SparkConf:
    """Set up the SparkContext with appropriate config for test.

    Parameters:
        app_name: Application name to provide to the SparkSession.
        conf: Optional SparkConf to be extended. Otherwise, creates a new SparkConf.

    Returns:
        SparkConf construct.

    """
    if conf is None:
        conf = SparkConf()

    # Common settings.
    conf.setAppName(app_name)
    conf.set("spark.ui.port", "4050")
    conf.set("spark.logConf", "true")
    conf.set("spark.debug.maxToStringFields", "100")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.sql.jsonGenerator.ignoreNullFields", "false")

    return conf


def aws_spark_conf(conf: Optional[SparkConf] = None) -> SparkConf:
    """AWS authentication config.

    Parameters:
        conf: Optional SparkConf to be extended. Otherwise, creates a new SparkConf.

    Returns:
        A SparkConf instance with AWS auth support.

    """
    if conf is None:
        conf = SparkConf()

    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-2.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.aws.experimental.input.fadvise", "random")
    aws_path = os.path.join(pathlib.Path.home(), ".aws", "credentials")
    if os.path.exists(aws_path):
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        with open(aws_path, encoding="utf-8") as _fh:
            aws_config = ConfigParser()
            aws_config.read_file(_fh)
            conf.set(
                "spark.hadoop.fs.s3a.access.key",
                aws_config.get("default", "aws_access_key_id"),
            )
            conf.set(
                "spark.hadoop.fs.s3a.secret.key",
                aws_config.get("default", "aws_secret_access_key"),
            )
            conf.set(
                "spark.hadoop.fs.s3a.session.token",
                aws_config.get("default", "aws_session_token"),
            )
    else:
        kms_key_arn = os.environ.get("KMS_KEY_ARN")
        if kms_key_arn:
            conf.set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
            )
            conf.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
            conf.set("spark.hadoop.fs.s3a.server-side-encryption.key", kms_key_arn)

    return conf


def spark_session(
    app_name: Text = diffit.__app_name__, conf: Optional[SparkConf] = None
) -> SparkSession:
    """SparkSession."""
    return SparkSession.builder.config(
        conf=spark_conf(app_name=app_name, conf=conf)
    ).getOrCreate()


def parquet_writer(
    dataframe: DataFrame, outpath: Text, mode: Mode = Mode.OVERWRITE
) -> None:
    """Write out Spark DataFrame `dataframe` to `outpath` directory as Spark Parquet.

    The write mode is defined by `mode`.

    """
    log.info("Writing Parquet to location: %s", outpath)
    dataframe.write.mode(mode).parquet(outpath)


def parquet_reader(spark: SparkSession, source_path: Text) -> DataFrame:
    """Read in Spark Parquet files from *source_path* directory.

    Returns a Spark SQL DataFrame.

    """
    log.info('Reading Parquet data from "%s"', source_path)

    return spark.read.parquet(source_path)


def json_writer(source: DataFrame, outpath: Text, mode: Mode = Mode.OVERWRITE) -> None:
    """Write out Spark DataFrame `source` to `outpath` directory as JSON.
    The write mode is defined by `mode`.

    """
    source.write.mode(mode).json(outpath)


def json_reader(
    spark: SparkSession, source_path: Text, schema: StructType, multiline: bool = False
) -> DataFrame:
    """Read in JSON files from *source_path* directory.

    Here, we leave nothing to chance. So you must provide a *schema*.

    Returns a Spark SQL DataFrame.

    """
    return (
        spark.read.schema(schema)
        .option("multiline", multiline)
        .option("mode", "FAILFAST")
        .json(source_path)
    )


def csv_reader(
    spark: SparkSession,
    schema: StructType,
    csv_path: Text,
    delimiter: Text = ",",
    header: bool = True,
) -> DataFrame:
    """Spark CSV reader.

    Setting such as *delimiter* and *header* can be adjusted during the read.

    Returns a DataFrame representation of the CSV.

    """
    return (
        spark.read.schema(schema)
        .option("delimiter", delimiter)
        .option("ignoreTrailingWhiteSpace", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("header", header)
        .option("emptyValue", None)
        .option("quote", '"')
        .csv(csv_path)
    )


def split_dir(directory_path: Text, directory_token: Text) -> Optional[Text]:
    """Helper function to strip leading directory parts from `directory*
    until *directory_token* is matched.

    Returns:
        The remaining parts of `directory_path` as a string.

    """
    directory_parts = deque(directory_path.split(os.sep))

    new_path = None
    while directory_parts:
        if directory_parts.popleft() == directory_token:
            new_path = os.path.join(*directory_parts)
            break

    return new_path


def sanitise_columns(
    source: DataFrame, problematic_chars: Text = ",;{}()="
) -> DataFrame:
    """As the diffit engine produces a parquet output, we may need
    to remove special characters from the source headers that do not
    align with the parquet conventions. The column sanitise step will:

    - convert to lower case
    - replace spaces with under-score
    - strip out the *problematic_char* set

    Returns:
        New DataFrame with adjusted columns.

    """
    new_columns = []

    for column in source.columns:
        column = column.lower()
        column = column.replace(" ", "_")
        for _char in problematic_chars:
            column = column.replace(_char, "")
        new_columns.append(column)

    return source.toDF(*new_columns)
