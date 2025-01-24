import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO


def save_json_to_parquet(bucket_name, json_data, output_file):
    # Convert JSON data to a Pandas DataFrame
    df = pd.DataFrame(json_data["values"])

    # Convert DataFrame to Apache Arrow Table
    table = pa.Table.from_pandas(df)

    # Write the table to a Parquet file in memory
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    # AWS S3 configuration
    s3_client = boto3.client('s3')
    object_key = f"{json_data['filename']}.parquet"

    # Upload the Parquet file to S3
    s3_client.upload_fileobj(parquet_buffer, bucket_name, object_key)
    return f"s3://{bucket_name}/{object_key}"

def read_parquet_to_json(input_file):
    """
    Read a Parquet file and convert it back to JSON.

    Parameters:
        input_file (str): Path to the input Parquet file.

    Returns:
        dict: JSON structure with the original 'data' key.
    """
    # Read the Parquet file
    df = pd.read_parquet(input_file, engine="pyarrow")

    # Convert back to JSON
    output_json = {"data": df.to_dict(orient="list")}

    return output_json


# Example usage
if __name__ == "__main__":
    # Input JSON
    input_json = {
        "data": {
            "col1": [1, 2, 3],
            "col2": [35, 687, 79]
        }
    }

    # File paths
    parquet_file = "output.parquet"

    # Save JSON to Parquet
    print(save_json_to_parquet(input_json, parquet_file))

    # Read Parquet back to JSON
    output_json = read_parquet_to_json(parquet_file)
    print(json.dumps(output_json, indent=4))
