import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Create a Spark session
spark = SparkSession.builder \
    .appName("Merge CSVs with Mapping") \
    .getOrCreate()

glueContext = GlueContext(spark)

# Define S3 paths
bucket_name = 'demo-bucket-flatten-json-files'
users_path = f's3://{bucket_name}/output/flattened_users.csv'
orders_path = f's3://{bucket_name}/output/flattened_orders.csv'
products_path = f's3://{bucket_name}/output/flattened_products.csv'
schema_path = f's3://{bucket_name}/schema.csv'  # Path to your final schema CSV
merged_data_path = f's3://{bucket_name}/merged_data/merged_data.json'

# Step 1: Read the CSV files into DataFrames
users_df = spark.read.option("header", "true").csv(users_path)
orders_df = spark.read.option("header", "true").csv(orders_path)
products_df = spark.read.option("header", "true").csv(products_path)

# Step 2: Read the final schema from S3
schema_df = spark.read.option("header", "true").csv(schema_path)

# Step 3: Join the DataFrames based on specified keys
# Join orders with users on user_id and customer_id
merged_df = orders_df.join(users_df, orders_df.customer_id == users_df.user_id, how='left')

# Join with products data on product_name
merged_df = merged_df.join(products_df, merged_df.product_name == products_df.product_name, how='left')

# Step 4: Select and map the required columns based on the provided mapping
final_df = merged_df.select(
    users_df["user_id"].alias("user_id"),
    (users_df["`name.first_name`"] + " " + users_df["`name.last_name`"]).alias("full_name"),
    users_df["`contact.email`"].alias("email"),
    orders_df["order_id"].alias("order_id"),
    orders_df["order_date"].alias("order_date"),
    orders_df["item_id"].alias("item_id"),
    orders_df["product_name"].alias("item_product_name"),
    orders_df["quantity"].alias("quantity"),
    orders_df["price"].alias("price"),
    orders_df["total_amount"].alias("total_amount"),
    products_df["product_name"].alias("product_product_name"),
    products_df["category"].alias("category"),
    products_df["stock_quantity"].alias("stock_quantity")
)

# Step 5: Write the merged DataFrame as JSON to S3
final_df.write.json(merged_data_path, mode='overwrite')

print(f"Merged data has been written to {merged_data_path}")

# Step 6: Create the Glue Data Catalog table
database_name = 'json_data_db'  # Replace with your Glue database name
table_name = 'merged_data'

# Create a Glue client
glue_client = boto3.client('glue')

# Step 7: Check if the Glue database exists, if not, create it
try:
    glue_client.get_database(Name=database_name)
    print(f"Database '{database_name}' already exists.")
except glue_client.exceptions.EntityNotFoundException:
    print(f"Database '{database_name}' not found. Creating the database.")
    glue_client.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Database for storing merged JSON data from S3',
            'LocationUri': f's3://{bucket_name}/'
        }
    )
    print(f"Database '{database_name}' created.")

# Define the Glue table schema
schema = [
    {"Name": "user_id", "Type": "string"},
    {"Name": "full_name", "Type": "string"},
    {"Name": "email", "Type": "string"},
    {"Name": "order_id", "Type": "string"},
    {"Name": "order_date", "Type": "string"},
    {"Name": "item_id", "Type": "string"},
    {"Name": "item_product_name", "Type": "string"},
    {"Name": "quantity", "Type": "int"},  # Assuming quantity is an integer
    {"Name": "price", "Type": "float"},    # Assuming price is a float
    {"Name": "total_amount", "Type": "float"},  # Assuming total_amount is a float
    {"Name": "product_product_name", "Type": "string"},
    {"Name": "category", "Type": "string"},
    {"Name": "stock_quantity", "Type": "int"}   # Assuming stock_quantity is an integer
]

# Create the Glue table
response = glue_client.create_table(
    DatabaseName=database_name,
    TableInput={
        'Name': table_name,
        'Description': 'Merged data table from users, orders, and products',
        'StorageDescriptor': {
            'Columns': schema,
            'Location': merged_data_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'TableType': 'EXTERNAL_TABLE'
    }
)

print(f"Table '{table_name}' created in Glue Data Catalog")