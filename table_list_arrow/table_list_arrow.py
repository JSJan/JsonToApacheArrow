import pyarrow as pa
import pyarrow.ipc as ipc
import io

# Create two Arrow tables
def create_tables():
    # Table 1 data
    data1 = {
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    }
    table1 = pa.table(data1)

    # Table 2 data
    data2 = {
        'col1': [4, 5, 6],
        'col2': ['d', 'e', 'f']
    }
    table2 = pa.table(data2)

    return table1, table2


def create_varying_column_tables():
    # Table 1 data
    data1 = {
        'col1': [1, 2, 3],
        'col3': ['a', 'b', 'c']
    }
    table1 = pa.table(data1)

    # Table 2 data
    data2 = {
        'col1': [4, 5, 6],
        'col4': ['d', 'e', 'f']
    }
    table2 = pa.table(data2)

    return table1, table2

# Stream Arrow Tables
def stream_arrow_tables(tables):
    # Create a BytesIO stream to hold the data
    stream = io.BytesIO()

    # Write multiple tables to the stream
    for table in tables:
        with ipc.new_file(stream, table.schema) as writer:
            writer.write(table)

    # Return the stream's content as bytes
    return stream.getvalue()

# Stream Arrow Tables Separately as stream list
def stream_arrow_tables_separately(tables):
    streams = []
    for table in tables:
        stream = io.BytesIO()
        with ipc.new_file(stream, table.schema) as writer:
            writer.write(table)
        streams.append(stream.getvalue())
    return streams


# Function to deserialize multiple streams
def deserialize_arrow_tables(streams):
    tables = []
    for stream_data in streams:
        stream = io.BytesIO(stream_data)  # Convert bytes back to a stream
        reader = ipc.RecordBatchFileReader(stream)
        tables.append(reader.read_all())  # Read entire table
    return tables

# Main function to demonstrate
def main():
    # Create tables
    #table1, table2 = create_tables()

    #varying column tables - Tried to write record batch with different schema
    table1, table2 = create_varying_column_tables()

    # Stream tables
    streamed_data = stream_arrow_tables_separately([table1, table2])

    # You can now send `streamed_data` over a network, save it to a file, or process it
    print("Streamed Data (bytes):")
    #print(streamed_data[:100])  # Print the first 100 bytes for demonstration
    print(streamed_data)

    # Deserialize tables separately
    deserialized_tables = deserialize_arrow_tables(streamed_data)

    # Print results
    for i, table in enumerate(deserialized_tables):
        print(f"Deserialized Table {i + 1}:")
        print(table)

if __name__ == "__main__":
    main()
