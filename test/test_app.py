import pytest
from io import BytesIO
import pandas as pd
from pyarrow import Table, ipc
from app import arrow_response_to_json

def test_arrow_response_to_json():
    # Create a sample Arrow Table
    data = {
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    }
    table = Table.from_pandas(pd.DataFrame(data))

    # Serialize the Arrow Table to a BytesIO buffer
    buffer = BytesIO()
    with ipc.new_file(buffer, table.schema) as writer:
        writer.write(table)
    buffer.seek(0)

    # Get the binary content of the Arrow Table
    response_content = buffer.getvalue()

    # Call the function to test
    json_output = arrow_response_to_json(response_content)

    # Expected JSON output
    expected_json = '[{"column1":1,"column2":"a"},{"column1":2,"column2":"b"},{"column1":3,"column2":"c"}]'

    # Assert the output is as expected
    assert json_output == expected_json

def test_arrow_response_to_json_empty_table():
    # Create an empty Arrow Table
    table = Table.from_pandas(pd.DataFrame())

    # Serialize the Arrow Table to a BytesIO buffer
    buffer = BytesIO()
    with ipc.new_file(buffer, table.schema) as writer:
        writer.write(table)
    buffer.seek(0)

    # Get the binary content of the Arrow Table
    response_content = buffer.getvalue()

    # Call the function to test
    json_output = arrow_response_to_json(response_content)

    # Expected JSON output for an empty table
    expected_json = '[]'

    # Assert the output is as expected
    assert json_output == expected_json

def test_arrow_response_to_json_invalid_content():
    # Create invalid binary content
    response_content = b'invalid content'

    # Call the function to test and expect a ValueError
    with pytest.raises(ValueError, match="Error deserializing Arrow data to JSON"):
        arrow_response_to_json(response_content)