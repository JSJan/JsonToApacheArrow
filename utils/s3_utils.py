import boto3
import pandas as pd
from io import BytesIO

s3_client = boto3.client("s3")

def upload_parquet_to_s3(bucket_name, object_key, df):
    """Uploads a Pandas DataFrame to S3 in Parquet format."""
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket_name, object_key)
    return f"s3://{bucket_name}/{object_key}"

def download_parquet_from_s3(bucket_name, object_key):
    """Downloads a Parquet file from S3 and returns it as a Pandas DataFrame."""
    buffer = BytesIO()
    s3_client.download_fileobj(bucket_name, object_key, buffer)
    buffer.seek(0)
    df = pd.read_parquet(buffer, engine="pyarrow")
    return df