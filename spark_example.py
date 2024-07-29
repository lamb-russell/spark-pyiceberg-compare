# create a sample iceberg table using spark.  using spark 3.5
# to run this, need to download the JAR file: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar
# info on the jar file here: https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2
# then use this command to run the python script: spark-submit --jars ~/Downloads/iceberg-spark-runtime-3.5_2.12-1.4.2.jar spark_example.py


import logging
logging.getLogger("pyspark").setLevel(logging.ERROR)

import urllib.request
from pyspark.sql import SparkSession

# Create a Spark session with a distinct warehouse path and Iceberg configurations
spark = SparkSession.builder \
    .appName("Spark Iceberg Example") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/spark_iceberg_warehouse") \
    .config("spark.sql.defaultCatalog", "local") \
    .config("spark.sql.legacy.createHiveTableByDefault", "false") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .master("local[1]") \
    .getOrCreate()

# Parquet file URL
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

# Local file path to save the downloaded Parquet file
local_file_path = "/tmp/yellow_tripdata_2023-01.parquet"

# Download the Parquet file
urllib.request.urlretrieve(parquet_url, local_file_path)

# Read the downloaded Parquet file into a DataFrame
df = spark.read.format("parquet").load(local_file_path)

# Write the DataFrame as an Iceberg table
df.writeTo("local.default.spark_iceberg_table").createOrReplace()

print("Spark Iceberg table created successfully!")

# Read and show the data
spark.table("local.default.spark_iceberg_table").show()

print("\nSpark Iceberg Warehouse location:")
print("/tmp/spark_iceberg_warehouse")

# Stop the Spark session
spark.stop()
