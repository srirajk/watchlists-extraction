import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit, concat_ws, expr, udf, when
from pyspark.sql.types import StringType
from src.ofac.custom_udfs import extract_names

from src.ofac.utility import load_config, pretty_print_spark_df

config = load_config()

# Paths to data
source_data_base_path = config['source_data_base_path']
reference_data_base_path = config['reference_data_base_path']
output_base_path = config['output_base_path']
warehouse_base_dir = config['warehouse_base_dir']

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


#For demo purpose only
spark.sql("DROP TABLE IF EXISTS gold.ofac_cdm PURGE").show()


spark.sql("CREATE DATABASE IF NOT EXISTS gold")


ofac_enriched_df = spark.read.format("iceberg").table("silver.ofac_enriched")
ofac_enriched_df.show(truncate=False)

current_ts = current_timestamp()

# Register the UDF
extract_names_udf = udf(extract_names, StringType())

# Transformations for Gold
gold_df = ofac_enriched_df.select(
    col("profile_id").alias("OFAC_RISK_ID"),  # Map profile_id directly
    col("app_profile_id").alias("UNIQUE_RISK_ID"),  # Use app-specific profile_id
    col("alias_id").alias("ALIAS_ID"),  # Map alias_id directly
    lit("OFAC SDN").alias("ITEM_OWNER"),  # Hardcoded value
    lit("SDN").alias("OFAC_TYPE"),  # Hardcoded value
    expr("CASE WHEN party_type = 'Individual' THEN 'I' ELSE 'E' END").alias("RECORD_TYPE"),  # Derive record type
    extract_names_udf(col("documented_names")).alias("NAME"),  # Extract and concatenate names using UDF
    col("id_documents").alias("DOCUMENTS"),  # Map directly
    lit("Downloaded from US Treasury Website").alias("DESCRIPTION"),  # Hardcoded description
    current_ts.alias("LAST_UPDATE_DATE"),  # Capture current timestamp
    when(
        (col("party_type") == "Entity") & (~col("party_sub_type").isin("Vessel", "Aircraft")),
        lit("Company")
    ).otherwise(col("party_sub_type")).alias("RECORD_SUB_TYPE")  # Default to party_sub_type if not matched
)

gold_df.show(truncate=False, vertical=True)


pretty_print_spark_df(gold_df, limit=100)

# Write to Gold
gold_df.writeTo("gold.ofac_cdm") \
    .createOrReplace()

# Stop the Spark Session
spark.stop()


