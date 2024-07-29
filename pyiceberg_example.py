import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
import duckdb
import os


warehouse_path = "/tmp/pyiceberg_warehouse"

try: # make the warehouse path if it doesn't exist
    os.makedirs(warehouse_path, exist_ok=True)
    print("Directory created or already exists.")
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

# create the iceberg table

from pyiceberg.exceptions import NoSuchNamespaceError, NamespaceAlreadyExistsError, TableAlreadyExistsError


# Define the table name and namespace
namespace = "taxi_namespace"
table_name = "taxi_trips"

# Create the namespace if it doesn't exist
try:
    catalog.create_namespace(namespace)
except NamespaceAlreadyExistsError:
    print("Namespace already exists.")
    pass  # Namespace already exists

# Convert the Arrow schema to PyIceberg schema
iceberg_schema = arrow_table.schema

# Create or load the Iceberg table
try:
    table = catalog.create_table(f"{namespace}.{table_name}", iceberg_schema)
except TableAlreadyExistsError:
    table = catalog.load_table(f"{namespace}.{table_name}")
    print("Iceberg table already exists.")

table.overwrite(arrow_table)  # Write the Arrow table to the Iceberg table