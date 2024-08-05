import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
import duckdb
import os
import pandas as pd
import shutil

warehouse_path = "/tmp/pyiceberg_warehouse"

# Delete the warehouse directory if it exists
if os.path.exists(warehouse_path):
    shutil.rmtree(warehouse_path)
    print(f"Deleted existing directory: {warehouse_path}")

# Create the warehouse directory
try:
    os.makedirs(warehouse_path)
    print(f"Created new directory: {warehouse_path}")
except Exception as e:
    print(f"Error creating directory: {e}")

catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

# Read the Parquet file using PyArrow
arrow_table = duckdb.sql("select * from read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet')").arrow()

from pyiceberg.exceptions import NoSuchNamespaceError, NamespaceAlreadyExistsError, TableAlreadyExistsError

# Define the table name and namespace
namespace = "taxi_namespace"
table_name = "taxi_trips"

# Create the namespace
try:
    catalog.create_namespace(namespace)
    print(f"Created namespace: {namespace}")
except NamespaceAlreadyExistsError:
    print(f"Namespace already exists: {namespace}")

# Convert the Arrow schema to PyIceberg schema
iceberg_schema = arrow_table.schema

# Create the Iceberg table
try:
    table = catalog.create_table(f"{namespace}.{table_name}", iceberg_schema)
    print(f"Created Iceberg table: {namespace}.{table_name}")
except TableAlreadyExistsError:
    table = catalog.load_table(f"{namespace}.{table_name}")
    print(f"Loaded existing Iceberg table: {namespace}.{table_name}")

table.overwrite(arrow_table)  # Write the Arrow table to the Iceberg table
print("Data written to Iceberg table")

# Query the Iceberg table
query_result = table.scan()

# Convert the query result to a pandas DataFrame
df = query_result.to_pandas()

# Display the first few rows of the DataFrame
print("\nFirst few rows of the DataFrame:")
print(df.head())

# Display additional information about the DataFrame
print("\nDataFrame Info:")
df.info()

print("\nDataFrame Summary Statistics:")
print(df.describe())