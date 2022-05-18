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

import diffit.files


def default_path_to_schemas():
    """Default location of the hard-wired JSON schema definitions.

    """
    def get_path():
        return os.path.join(pathlib.Path(__file__).resolve().parents[0], 'data')

    return get_path()


def interpret_schema(path_to_schema: str) -> StructType:
    """Generate a :class:`pyspark.sql.types.StructType` from source JSON defined by
    path at `path_to_schema`.

    """
    logging.info('Parsing Spark DataFrame schema from "%s"', path_to_schema)
    try:
        with open(path_to_schema, encoding='utf-8') as _fh:
            schema = StructType.fromJson(json.loads(_fh.read()))
    except NotADirectoryError:
        # Hard-wired py-files for spark-submit.
        deps_name = 'dependencies.zip'
        deps_path = os.path.join(os.sep, 'data', deps_name)
        with zipfile.ZipFile(deps_path, 'r') as myzip:
            with myzip.open(diffit.files.split_dir(path_to_schema, deps_name)) as _fh:
                schema = StructType.fromJson(json.loads(_fh.read()))

    return schema


def names(path_to_schemas: str = None,
          file_filter: str = '*.json') -> Union[Iterator[Tuple[int, str]], None]:
    """Schemas are typically sourced as JSON constructs under
    *path_to_schema*.  The JSON file name will act as the schema name.

    """
    if not path_to_schemas:
        path_to_schemas = default_path_to_schemas()

    logging.info('Checking for schemas under: %s', path_to_schemas)
    schema_paths = filester.get_directory_files_list(path_to_schemas, file_filter=file_filter)
    schema_names = [(os.path.splitext(os.path.basename(x))[:-1], x) for x in schema_paths]
    for schema in schema_names:
        yield schema


def get(schema_name: str, path_to_schemas: str = None) -> Union[StructType, None]:
    """Helper to get the Spark SQL schema definition from a given *schema_name*.
    *schema_name* should exist in the list of supported schema :func:`names`
    otherwise `None` is returned.

    """
    if not path_to_schemas:
        path_to_schemas = default_path_to_schemas()

    schema_struct = None
    try:
        schema_struct = interpret_schema(os.path.join(path_to_schemas, f'{schema_name}.json'))
    except FileNotFoundError as err:
        logging.error(err)

    return schema_struct
