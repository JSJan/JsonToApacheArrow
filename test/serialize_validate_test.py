import pyarrow as pa
import pyarrow.ipc

# Example Data
data = {
    "day": [1, 2],
    "acEfficiency": [95, 97],
    "dcEfficiency": [90, 92],
    "fooBar": [True, False],
}

# Create Arrow Table
table = pa.Table.from_pydict(data)

# Serialize Table to IPC Format
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, table.schema) as writer:
    writer.write(table)
arrow_response = sink.getvalue().to_pybytes()

print("Arrow table serialized to binary format:")
print(arrow_response)

print("\nArrow table serialized to hexadecimal format:")
print(arrow_response.hex())

try:
    buf = pa.BufferReader(arrow_response)
    stream = pa.ipc.open_stream(buf)
    table = stream.read_all()
    print("Table Schema:", table.schema)
    print("Table Data:", table.to_pandas())
except Exception as e:
    print(f"Inspection Error: {e}")