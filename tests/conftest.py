"""Global fixture arrangement.

"""
import tempfile
import shutil
import logging
import pytest
import pyspark


@pytest.fixture(scope='session')
def spark_context(request) -> pyspark.SparkContext:
    """Set up the SparkContext with appropriate config for test.

    Only one SparkContext should be active per JVM.  You must stop()
    the active SparkContext before creating a new one.

    SparkContext instance is not supported to share across multiple processes
    out of the box and PySpark does not guarantee multi-processing execution.
    Use threads instead for concurrent processing purpose.

    """
    def fin():
        """Grab the SparkContext (if any) and stop it.
        """
        _sc = pyspark.SparkContext.getOrCreate()
        _sc.stop()

    conf = pyspark.SparkConf()
    conf.setAppName('test')
    conf.set('spark.executor.memory', '2g')
    conf.set('spark.executor.cores', '2')
    conf.set('spark.cores.max', '10')
    conf.set('spark.ui.port', '4050')
    conf.set('spark.logConf', True)
    conf.set('spark.debug.maxToStringFields', 100)

    _sc = pyspark.SparkContext(conf=conf)

    request.addfinalizer(fin)

    return _sc


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
