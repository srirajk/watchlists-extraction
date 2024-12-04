import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit

from src.ofac.schemas import distinct_party_schema

# Paths to data
source_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/source_data/ofac/"
reference_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/reference_data/ofac/"
output_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/output/ofac/"
warehouse_base_dir = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/iceberg-warehouse"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ofac_bronze_compute") \
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.databricks:spark-xml_2.12:0.18.0') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", f"{warehouse_base_dir}/catalog") \
    .config("spark.local.dir", f"{warehouse_base_dir}/tmp") \
    .config("spark.sql.warehouse.dir", f"{warehouse_base_dir}/data") \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()

print("Printing the schema of gold.ofac_cdm")
spark.sql("describe extended gold.ofac_cdm").show()

print("Distinct values of RECORD_TYPE")
spark.sql("select distinct RECORD_SUB_TYPE from gold.ofac_cdm").show()

print("Distinct values of RECORD_TYPE where RECORD_SUB_TYPE is Unknown")
spark.sql("select distinct RECORD_TYPE from gold.ofac_cdm where RECORD_SUB_TYPE == 'Unknown' ").show()

print("List records where RECORD_TYPE is Individual")
spark.sql("select * from gold.ofac_cdm where RECORD_TYPE == 'I' ").show()

# Stop the Spark Session
spark.stop()
