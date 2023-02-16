"""Global fixture arrangement.

"""
from typing import Text
import shutil
import tempfile

from logga import log
from pyspark import SparkConf
from pyspark.sql import SparkSession
import _pytest.fixtures
import pytest

import diffit.datastore.spark


@pytest.fixture(scope="session")
def spark(driver_memory: Text = "1g") -> SparkSession:
    """Handler to the SparkSession for the test harness."""
    conf = SparkConf()
    conf.set("spark.driver.memory", driver_memory)

    return diffit.datastore.spark.spark_session(app_name="test", conf=conf)


@pytest.fixture
def working_dir(request: _pytest.fixtures.SubRequest) -> Text:
    """Temporary working directory."""

    def fin() -> None:
        """Tear down."""
        log.info('Deleting temporary test directory: "%s"', dirpath)
        shutil.rmtree(dirpath)

    request.addfinalizer(fin)
    dirpath = tempfile.mkdtemp()
    log.info('Created temporary test directory: "%s"', dirpath)

    return dirpath
