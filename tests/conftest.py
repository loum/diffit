"""Global fixture arrangement.

"""
import logging
import shutil
import tempfile

import pyspark
import pytest

import diffit.datasources.spark


@pytest.fixture(scope='session')
def spark() -> pyspark.sql.SparkSession:
    """Handler to the SparkSession for the test harness.
    """
    return diffit.datasources.spark.spark_session(app_name='test')


@pytest.fixture
def working_dir(request):
    """Temporary working directory.
    """
    def fin():
        """Tear down.
        """
        logging.info('Deleting temporary test directory: "%s"', dirpath)
        shutil.rmtree(dirpath)

    request.addfinalizer(fin)
    dirpath = tempfile.mkdtemp()
    logging.info('Created temporary test directory: "%s"', dirpath)

    return dirpath
