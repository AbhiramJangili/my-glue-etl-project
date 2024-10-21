import json
import pandas as pd
import boto3
import io

# Initialize the S3 client
s3 = boto3.client('s3')

# Function to read JSON from S3, flatten it, and write the output back to S3
def process_s3_json(input_bucket, input_key, output_bucket, output_key):
    # Step 1: Read the JSON file from S3
    try:
        response = s3.get_object(Bucket=input_bucket, Key=input_key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)
        
        # Step 2: Normalize (flatten) the nested JSON data using pandas.json_normalize
        flattened_data = pd.json_normalize(data['products'])  # Modify this according to your JSON structure
        
        # Step 3: Write the flattened data to an in-memory CSV
        csv_buffer = io.StringIO()
        flattened_data.to_csv(csv_buffer, index=False, header=True)

        # Step 4: Upload the CSV file to the output S3 bucket
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())

        print(f"Flattened data has been successfully written to s3://{output_bucket}/{output_key}")

    except Exception as e:
        print(f"Error processing S3 file: {str(e)}")

# Set your S3 input and output details
input_bucket = 'demo-bucket-flatten-json-files'  # Replace with your S3 bucket
input_key = 'products.json'  # Replace with your JSON file path in S3
output_bucket = 'demo-bucket-flatten-json-files'  # Replace with your output S3 bucket
output_key = 'output/flattened_products.csv'  # Desired output file path in S3

# Call the function to process the JSON
process_s3_json(input_bucket, input_key, output_bucket, output_key)
