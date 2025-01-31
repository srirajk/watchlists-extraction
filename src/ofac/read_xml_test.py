import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, ArrayType
from collections import defaultdict

# Initialize Spark Session
appName = "OFAC_REFERENCE_DATA_EXAMPLE"
spark = SparkSession.builder \
    .master("local[1]") \
    .appName(appName) \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
    .getOrCreate()

# Define base path for XML file
source_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/source_data/ofac/"
xmlFilePath = source_data_base_path + "sdn_advanced.xml"

reference_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/reference_data/ofac/"


# Define the output file path
output_file_path = f"{reference_data_base_path}/reference_values_map.json"

# Define the schema if known, otherwise Spark will infer it
# Reading XML Data
aliasTypeValues_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "AliasTypeValues") \
    .option("valueTag", "_VALUE") \
    .load(xmlFilePath)


aliasTypeValues_df.printSchema()

aliasTypeValues_df.show(5, truncate=False)


spark.stop()