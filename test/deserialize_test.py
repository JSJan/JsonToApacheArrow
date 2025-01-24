# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.
print("Try programiz.pro")

import pyarrow as pa
from pyarrow import ipc
from io import BytesIO
import pandas as pd

def deserialize_arrow_to_json(arrow_response: bytes) -> str:
    """
    Deserialize an Apache Arrow response string to JSON.

    Args:
        arrow_response (bytes): The binary response data in Apache Arrow format.

    Returns:
        str: JSON representation of the Arrow table.
    """
    try:
        # Wrap the response content in a BytesIO buffer
        buffer = BytesIO(arrow_response)

        # Deserialize the Arrow table from the buffer
        reader = ipc.open_stream(buffer)
        table = reader.read_all()

        # Convert the Arrow table to a Pandas DataFrame
        df = table.to_pandas()

        # Convert the DataFrame to JSON
        json_output = df.to_json(orient="records")  # List of records as JSON
        return json_output
    except Exception as e:
        raise ValueError(f"Error during deserialization: {e}")

# Example Usage
arrow_response = b'\xff\xff\xff\xff \x01\x00\x00\x10\x00\x00\x00\x00\x00\n\x00\x0c\x00\x06\x00\x05\x00\x08\x00\n\x00\x00\x00\x00\x01\x04\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00\xbc\x00\x00\x00p\x00\x00\x004\x00\x00\x00\x04\x00\x00\x00d\xff\xff\xff\x00\x00\x01\x06\x10\x00\x00\x00\x1c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00fooBar\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x90\xff\xff\xff\x00\x00\x01\x02\x10\x00\x00\x00 \x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00dcEfficiency\x00\x00\x00\x00\x8c\xff\xff\xff\x00\x00\x00\x01@\x00\x00\x00\xc8\xff\xff\xff\x00\x00\x01\x02\x10\x00\x00\x00 \x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00acEfficiency\x00\x00\x00\x00\xc4\xff\xff\xff\x00\x00\x00\x01@\x00\x00\x00\x10\x00\x14\x00\x08\x00\x06\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x01\x02\x10\x00\x00\x00\x1c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00day\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01@\x00\x00\x00\xff\xff\xff\xff\x18\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x04\x00\x18\x00\x00\x008\x00\x00\x00\x00\x00\x00\x00\x00\x00\n\x00\x18\x00\x0c\x00\x04\x00\x08\x00\n\x00\x00\x00\x9c\x00\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00_\x00\x00\x00\x00\x00\x00\x00a\x00\x00\x00\x00\x00\x00\x00Z\x00\x00\x00\x00\x00\x00\x00\\\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00'

try:
    json_result = deserialize_arrow_to_json(arrow_response)
    print("JSON Result:")
    print(json_result)
except ValueError as e:
    print(f"Error: {e}")