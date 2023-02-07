"""`diffit.schema` unit test cases.

"""
from typing import Text
import json
import tempfile
import os

from pyspark.sql.types import StructType

import diffit.schema


def test_interpret_schema(working_dir: Text) -> None:
    """Generate a pyspark.sql.types.StructType from JSON."""
    # Given a path to a JSON schema file
    schema = {
        "type": "struct",
        "fields": [
            {
                "name": "account_number",
                "type": "string",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "advertiser_id",
                "type": "string",
                "nullable": True,
                "metadata": {},
            },
        ],
    }
    with tempfile.NamedTemporaryFile(mode="w", dir=working_dir, delete=False) as _fh:
        json.dump(schema, _fh)
        path_to_schema = _fh.name

    # when I interpret the schema definition
    received = diffit.schema.interpret_schema(path_to_schema)

    # I should receive a Spark StructType
    msg = "Schema interpretation does not create a Spark StructType"
    assert isinstance(received, StructType), msg


def test_interpret_schema_from_missing_file(working_dir: Text) -> None:
    """Gracefully fail a pyspark.sql.types.StructType interpretation from missing file."""
    # Given a path to a missing JSON schema file
    path_to_schema = os.path.join(working_dir, "Missing.json")

    # when I interpret the schema definition
    received = diffit.schema.interpret_schema(path_to_schema)

    # then the schema interpreter should gracefully fail and return None
    msg = "Schema interpretation on missing file should return None"
    assert received is None, msg


def test_interpret_schema_from_bad_json(working_dir: Text) -> None:
    """Gracefully fail a pyspark.sql.types.StructType interpretation from bad JSON."""
    # Given a path to an invalid JSON schema file
    path_to_schema = None
    with tempfile.NamedTemporaryFile(mode="w", dir=working_dir, delete=False) as _fh:
        _fh.write('["Hello", 3.14, true, ]')
        path_to_schema = _fh.name

    # when I interpret the schema definition
    received = diffit.schema.interpret_schema(path_to_schema)

    # then the schema interpreter should gracefully fail and return None
    msg = "Schema interpretation on file with bad JSON should return None"
    assert received is None, msg
