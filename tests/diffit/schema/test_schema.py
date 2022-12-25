""":mod:`diffit.schema` unit test cases.

"""
import json
import tempfile
from pyspark.sql.types import StructType

import diffit.schema


def test_interpret_schema(working_dir):
    """Generate a pyspark.sql.types.StructType from JSON.
    """
    # Given a path to a JSON schema file
    schema = {
        'type': 'struct',
        'fields': [
            {
                'name' : 'account_number',
                'type' : 'string',
                'nullable': True,
                'metadata': {}
            },
            {
                'name': 'advertiser_id',
                'type': 'string',
                'nullable': True,
                'metadata': {}
            }
        ]
    }
    with tempfile.NamedTemporaryFile(mode='w', dir=working_dir, delete=False) as _fh:
        json.dump(schema, _fh)
        path_to_schema = _fh.name

    # when I interpret the schema definition
    received = diffit.schema.interpret_schema(path_to_schema)

    # I should receive a Spark StructType
    msg = 'Schema interpretation does not create a Spark StructType'
    assert isinstance(received, StructType), msg


def test_names():
    """Get the supported schemas.
    """
    # Given a set of Spark DataFrame JSON-type schema definitions
    schemas = [x[0][0] for x in diffit.schema.names()]

    # I should be able to retrieve the names
    msg = 'Schema names error'
    expected = ['Dummy']
    assert sorted(schemas) == expected, msg


def test_names_no_schema_definitions_available(working_dir):
    """Get the supported schemas: no schema definitions.
    """
    # Given an empty set of Spark DataFrame JSON-type schema definitions
    schemas = list(diffit.schema.names(working_dir))

    # no names should be returned (without error)
    msg = 'Schema names error: no schema definitions'
    assert not schemas, msg


def test_get():
    """Get a known schema definition.
    """
    # Given a valid and supported schema definition name
    schema_name = 'Dummy'

    # when I try to source the schema as a Spark StructType
    received = diffit.schema.get(schema_name)

    # then as a system I should receive the correct instance type
    msg = 'Source Spark Schema not a Spark StructType'
    assert isinstance(received, StructType), msg


def test_get_no_match():
    """Get a known schema definition: no match.
    """
    # Given an unsupported schema definition name
    schema_name = 'Banana'

    # when I try to source the schema as a Spark StructType
    # then as a system I should receive the correct instance type
    msg = 'Source Spark Schema not a Spark StructType'
    assert not diffit.schema.get(schema_name), msg
