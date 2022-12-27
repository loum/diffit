"""Schema utilities.
"""
import logging
import json
import os
import pathlib
import zipfile
from typing import (Tuple, Iterator, Union)
from pyspark.sql.types import StructType
import filester

import diffit.datasources.spark


def interpret_schema(path_to_schema: str) -> StructType:
    """Generate a :class:`pyspark.sql.types.StructType` from source JSON defined by
    path at `path_to_schema`.

    """
    logging.info('Parsing Spark DataFrame schema from "%s"', path_to_schema)

    schema = None
    try:
        with open(path_to_schema, encoding='utf-8') as _fh:
            schema = StructType.fromJson(json.loads(_fh.read()))
    except (FileNotFoundError, json.decoder.JSONDecodeError) as err:
        logging.warn('Schema interpretation error: %s', err)

    return schema
