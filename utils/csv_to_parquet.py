# def upload_file():
#     try:
#         # Get the file from the request
#         file = request.files['file']
        
#         # Read the file into a pandas DataFrame
#         df = pd.read_csv(file)
        
#         # Convert the DataFrame to CSV and save it as an in-memory string
#         csv_buffer = StringIO()
#         df.to_csv(csv_buffer, index=False)
#         csv_buffer.seek(0)
        
#         # Upload the file to S3
#         s3_client.put_object(
#             Bucket=BUCKET_NAME,
#             Key=file.filename,
#             Body=csv_buffer.getvalue()
#         )
        
#         return jsonify({"message": "File uploaded successfully!"}), 200
#     except Exception as e:
#         return jsonify({"error": str(e)}), 400

# # Download endpoint for columnar data
# @app.route('/download/<filename>', methods=['GET'])
# def download_file(filename):
#     try:
#         # Retrieve the file from S3
#         response = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)
        
#         # Get the file content
#         data = response['Body'].read().decode('utf-8')
        
#         # Convert the content to a DataFrame and return as CSV
#         df = pd.read_csv(StringIO(data))
#         return df.to_csv(index=False), 200
#     except Exception as e:
#         return jsonify({"error": str(e)}), 400

# if __name__ == '__main__':
#     app.run(debug=True)