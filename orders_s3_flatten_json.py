import json
import pandas as pd
import boto3
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')

# Set your bucket and key (file path in S3)
input_bucket = 'demo-bucket-flatten-json-files'
input_key = 'orders.json'  # Replace with your actual file path in S3
output_bucket = 'demo-bucket-flatten-json-files'
output_key = 'output/flattened_orders.csv'  # Desired output file path in S3

# Step 1: Read the JSON file from S3
response = s3.get_object(Bucket=input_bucket, Key=input_key)
data = json.loads(response['Body'].read().decode('utf-8'))

# Step 2: Normalize (flatten) the outer part of the JSON data
flattened_data = pd.json_normalize(data['orders'])  # Assuming 'orders' is the key for the list of orders

# Step 3: Explode the 'items' column, which contains lists of items, so that each item gets its own row
flattened_data['items'] = flattened_data['items'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
flattened_data = flattened_data.explode('items', ignore_index=True)

# Step 4: Normalize the exploded 'items' column into individual columns (e.g., 'item_id', 'product_name', 'quantity', 'price')
items_data = pd.json_normalize(flattened_data['items'])

# Step 5: Drop the old 'items' column and concatenate the new normalized items data
flattened_data = flattened_data.drop(columns=['items']).reset_index(drop=True)
flattened_data = pd.concat([flattened_data, items_data], axis=1)

# Step 6: Save the fully flattened data to CSV format
csv_buffer = StringIO()
flattened_data.to_csv(csv_buffer, index=False, header=True)

# Step 7: Upload the CSV data back to S3
s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())

print(f"Flattened data has been written to s3://{output_bucket}/{output_key}")
