# JsonToApacheArrow

**Setup**

`pip install -r requirements.txt
source env.local.sh - add your aws credentials
uvicorn app:app --reload`

bucket- created in consumption intelligence - z004763htest


`curl --location 'http://127.0.0.1:8000/apacheArrow/download/?filename=json_data' \
--header 'Accept: application/vnd.apache.arrow.stream'
-o output.arrow
`


**POST Request**

`{
    "message": "File uploaded successfully",
    "s3_path": "s3://z004763htest/uploads/user_data.parquet"
}`

POST 

`{
  "data": [
    {"Name": "Alice", "Age": 25, "City": "New York"},
    {"Name": "Bob", "Age": 30, "City": "Los Angeles"}
  ],
  "filename": "user_data"
}`

Response 

`{
  "message": "File uploaded successfully",
  "s3_path": "s3://your-bucket-name/uploads/user_data.parquet"
}`

GET /download/?filename=user_data

`[
  {"Name": "Alice", "Age": 25, "City": "New York"},
  {"Name": "Bob", "Age": 30, "City": "Los Angeles"}
]`

--------

### Returning multi part http response 

## use case 
- Return multiple parquet files as multipart response 
- Logic to deserialize the multipart response 

## Related files 
- test/deserialize_multipart.py 
- app.py/download_multipart


`curl --location 'http://127.0.0.1:8000/download-multipart' \
--header 'Accept: application/vnd.apache.arrow.stream'`