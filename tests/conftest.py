"""Global fixture arrangement.

"""
import shutil
import tempfile

from logga import log
from pyspark.sql import SparkSession
import pytest

import diffit.datasources.spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Handler to the SparkSession for the test harness."""
    return diffit.datasources.spark.spark_session(app_name="test")


@pytest.fixture
def working_dir(request):
    """Temporary working directory."""

    def fin():
        """Tear down."""
        log.info('Deleting temporary test directory: "%s"', dirpath)
        shutil.rmtree(dirpath)

    request.addfinalizer(fin)
    dirpath = tempfile.mkdtemp()
    log.info('Created temporary test directory: "%s"', dirpath)

    return dirpath
