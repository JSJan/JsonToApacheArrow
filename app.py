from fastapi import FastAPI,Request, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd
from utils.s3_utils import upload_parquet_to_s3, download_parquet_from_s3
from utils.json_to_parquet import save_json_to_parquet
from io import BytesIO
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.responses import StreamingResponse
from s3fs import S3FileSystem
from pyarrow import ipc

app = FastAPI()

# S3 Configurations
BUCKET_NAME = "z004763htest"
UPLOAD_FOLDER = "uploads/"
DOWNLOAD_FOLDER = "downloads/"

# Request Schema
class DataUploadRequest(BaseModel):
    data: List[dict]
    filename: str

@app.post("/upload/")
async def upload_data(request: DataUploadRequest):
    """
    Upload JSON data as a Parquet file to S3.
    """
    try:
        # Convert JSON to DataFrame
        df = pd.DataFrame(request.data)

        # Generate S3 object key
        object_key = f"{UPLOAD_FOLDER}{request.filename}.parquet"

        # Upload to S3
        s3_path = upload_parquet_to_s3(BUCKET_NAME, object_key, df)
        return {"message": "File uploaded successfully", "s3_path": s3_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/")
async def download_data(filename: str):
    """
    Download a Parquet file from S3 and return it as JSON.
    """
    try:
        # Generate S3 object key
        object_key = f"{UPLOAD_FOLDER}{filename}.parquet"

        # Download from S3
        df = download_parquet_from_s3(BUCKET_NAME, object_key)

        # Convert DataFrame to JSON
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/jsonData/upload/")
async def upload_data(request: Request):
    """
    Upload JSON data as a Parquet file to S3.
    """
    try:
       
        print(request)
        data = await request.json()  # Access JSON payload from the request
        print(data)
        #return {"message": "Received!", "data": data} 

        filename = data.get('filename', 'default')
       # Generate S3 object key
        object_key = f"s3://{BUCKET_NAME}/{UPLOAD_FOLDER}{filename}.parquet"

        # Upload to S3
        s3_path = save_json_to_parquet(BUCKET_NAME, data, object_key)
        return {"message": "File uploaded successfully", "s3_path": s3_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/apacheArrow/download/")
async def download_data(filename: str):
    """
    Download a Parquet file from S3 and return it as an Apache Arrow Table.
    """
    try:
        # Generate S3 object key
        object_key = f"{filename}.parquet"

        # Step 1: Download the Parquet file as a PyArrow Table
        table = download_parquet_from_s3_as_table(BUCKET_NAME, object_key)

        # Step 2: Serialize the PyArrow Table to Arrow binary format
        output_buffer = pa.BufferOutputStream()  # Create an Arrow output buffer
        with pa.ipc.new_stream(output_buffer, table.schema) as writer:
            writer.write_table(table)

        # Step 3: Get the binary data from the buffer
        arrow_binary = output_buffer.getvalue()

        # Step 4: Return the binary data as a streaming response
        return StreamingResponse(
            iter([arrow_binary.to_pybytes()]), 
            media_type="application/vnd.apache.arrow.stream",
            headers={"Content-Disposition": "attachment; filename=data.arrow"} #downloads the response as file in that case
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


def download_parquet_from_s3_as_table(bucket_name: str, object_key: str) -> pa.Table:
    try:
        s3 = S3FileSystem()
        with s3.open(f"s3://{bucket_name}/{object_key}", 'rb') as f:
            print(f"Downloading {object_key} from bucket {bucket_name}")
            file_size = f.size
            print(f"File size: {file_size} bytes")
            table = pq.read_table(f)
            print("Parquet file successfully read into Arrow Table")
            print("table", table)
            return table
    except Exception as e:
        logging.error(f"Error downloading Parquet file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def arrow_response_to_json(response_content: bytes) -> str:
    """
    Deserialize an Apache Arrow response string to JSON.

    Args:
        response_content (bytes): The binary content of the Arrow response.

    Returns:
        str: JSON representation of the Arrow table.
    """
    try:
        # Wrap the response content in a BytesIO buffer
        buffer = BytesIO(response_content)

        # Deserialize the Arrow table from the buffer
        reader = ipc.open_stream(buffer)
        table = reader.read_all()

        # Convert the Arrow table to a Pandas DataFrame
        df = table.to_pandas()

        # Convert the DataFrame to JSON
        json_output = df.to_json(orient="records")  # Produces a list of JSON objects
        return json_output
    except Exception as e:
        raise ValueError(f"Error deserializing Arrow data to JSON: {e}")