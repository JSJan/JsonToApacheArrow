import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Path to your Parquet file
parquet_file = "data/upload/json_data.parquet"

# Step 1: Read the Parquet file into a PyArrow Table
table = pq.read_table(parquet_file)

# Step 2: Serialize the table to Arrow binary format
output_buffer = pa.BufferOutputStream()  # Create a buffer to hold binary data
with pa.ipc.new_stream(output_buffer, table.schema) as writer:
    writer.write_table(table)

# Step 3: Get the binary data from the buffer
arrow_binary = output_buffer.getvalue()
print('get value', arrow_binary.to_pybytes())

# Step 4: Save or process the Arrow binary data
# Example: Save to a file
# with open("output.arrow", "wb") as f:
#     f.write(arrow_binary.to_pybytes())

# Output: Binary format
#print("Arrow table serialized to binary format:")
#print(arrow_binary)

# Output: Hexadecimal format for inspection
print("\nArrow table serialized to hexadecimal format:")
print(arrow_binary.hex())
