"""Schema utilities.

"""
from typing import Optional, Tuple, Iterator, Union
import json
import os
import pathlib
import zipfile

from logga import log
from pyspark.sql.types import StructType
import filester

import diffit.datastore.spark


def interpret_schema(path_to_schema: str) -> Optional[StructType]:
    """Generate a `pyspark.sql.types.StructType` from source JSON defined by
    path at `path_to_schema`.

    Returns:
        On success, a `StructType` schema representing the parsed JSON.

    """
    log.info('Parsing Spark DataFrame schema from "%s"', path_to_schema)

    schema: Optional[StructType] = None
    try:
        with open(path_to_schema, encoding="utf-8") as _fh:
            schema = StructType.fromJson(json.loads(_fh.read()))
    except (FileNotFoundError, json.decoder.JSONDecodeError) as err:
        log.error("Schema interpretation error: %s", err)

    return schema
