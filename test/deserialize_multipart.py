import pyarrow as pa
import pyarrow.ipc as ipc
import requests

def deserialize_arrow_streams(multipart_response):
    boundary = "boundary123"
    parts = multipart_response.split(f"--{boundary}".encode())[1:-1]  # Split and ignore the first and last boundary parts

    tables = []
    for part in parts:
        headers, stream = part.split(b"\r\n\r\n", 1)  # Split headers and stream data
        buffer = pa.py_buffer(stream.strip())  # Create a pyarrow.Buffer from the stream data
        reader = ipc.RecordBatchStreamReader(buffer)
        table = reader.read_all()
        tables.append(table)

    return tables

# Example usage
response = requests.get("http://localhost:8000/download-multipart")
arrow_tables = deserialize_arrow_streams(response.content)

for i, table in enumerate(arrow_tables):
    print(f"Table {i + 1}:")
    print(table)