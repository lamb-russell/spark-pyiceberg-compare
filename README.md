# spark-pyiceberg-compare

This project compares the usage of Apache Spark and Apache Iceberg libraries for reading and writing data in Python. It provides example scripts to demonstrate how to create an Iceberg table from a Parquet file using both Spark and PyIceberg.

## Prerequisites

- Python 3.x
- Apache Spark 3.5.x
- PyIceberg
- PyArrow
- DuckDB (for PyIceberg example)

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/your-username/spark-pyiceberg-compare.git
   ```

2. Install the required Python libraries:
   ```
   pip install pyspark pyiceberg pyarrow duckdb
   ```

3. Download the Iceberg Spark runtime JAR file:
   - URL: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar
   - Place the JAR file in a local directory (e.g., `~/Downloads/`).

## Usage

### Spark Example

1. Run the Spark example script:
   ```
   spark-submit --jars ~/Downloads/iceberg-spark-runtime-3.5_2.12-1.4.2.jar spark_example.py
   ```

   This script does the following:
   - Creates a Spark session with Iceberg configurations.
   - Downloads a Parquet file from a URL.
   - Reads the Parquet file into a DataFrame.
   - Writes the DataFrame as an Iceberg table.
   - Reads and displays the data from the Iceberg table.

### PyIceberg Example

1. Run the PyIceberg example script:
   ```
   python pyiceberg_example.py
   ```

   This script does the following:
   - Creates a SQLite catalog for Iceberg.
   - Downloads a Parquet file from a URL using DuckDB.
   - Converts the Arrow schema to PyIceberg schema.
   - Creates an Iceberg table and writes the data.
   - Reads and displays the data from the Iceberg table.

## Acknowledgements

- [Apache Spark](https://spark.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [PyIceberg](https://pyiceberg.readthedocs.io/)
- [PyArrow](https://arrow.apache.org/docs/python/)
- [DuckDB](https://duckdb.org/)