"""SparkSession as a data source.

"""
from pathlib import Path
from typing import Text
import os

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession

import diffit


def spark_conf(app_name: Text, conf: SparkConf = None) -> SparkConf:
    """Set up the SparkContext with appropriate config for test.

    """
    if conf is None:
        conf = SparkConf()

    # Common settings.
    conf.setAppName(app_name)
    conf.set('spark.ui.port', '4050')
    conf.set('spark.logConf', True)
    conf.set('spark.debug.maxToStringFields', 100)
    conf.set('spark.sql.session.timeZone', 'UTC')

    return conf


def spark_session(app_name: Text = diffit.__app_name__,
                  conf: SparkConf = None) -> SparkSession:
    """SparkSession.

    """
    return SparkSession.builder.config(conf=spark_conf(app_name=app_name, conf=conf)).getOrCreate()


def parquet_writer(dataframe: DataFrame, outpath: Text, mode='overwrite'):
    """Write out Spark DataFrame *dataframe* to *outpath* directory.

    The write mode is defined by *mode*.

    """
    dataframe.write.mode(mode).parquet(outpath)


def parquet_reader(spark: SparkSession, source_path: Text) -> DataFrame:
    """Read in Spark Parquet files from *source_path* directory.

    Returns a Spark SQL DataFrame.

    """
    return spark.read.parquet(source_path)
