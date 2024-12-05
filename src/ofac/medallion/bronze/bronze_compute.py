import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit

from src.ofac.schemas import distinct_party_schema, id_reg_documents_schema

from src.ofac.utility import load_config

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
spark.sql("DROP TABLE IF EXISTS bronze.distinct_parties PURGE").show()

spark.sql("DROP TABLE IF EXISTS bronze.identities PURGE").show()

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Read XML with schema
distinct_party_xml = source_data_base_path + "sdn_advanced.xml"
distinct_parties_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "DistinctParty") \
    .schema(distinct_party_schema) \
    .load(distinct_party_xml)

print(f" Start Distinct Parties Direct distinct_parties_df")
distinct_parties_df.show(truncate=False)
print(f" End Distinct Parties Direct distinct_parties_df")

# Explode the "Profile" array to flatten the structure
distinct_parties_profile_df = distinct_parties_df.select(
    col("_FixedRef"),
    col("_DeltaAction"),
    col("Comment"),
    explode(col("Profile")).alias("Profile")
)

print(f" Start Distinct Parties Explode Direct distinct_parties_profile_df")
distinct_parties_profile_df.show(truncate=False, vertical=True)
print(f" End Distinct Parties Explode Direct distinct_parties_profile_df")

current_ts = current_timestamp()

distinct_parties_profile_df = distinct_parties_profile_df.withColumn("ingest_timestamp", current_ts) \
    .withColumn("source_name", lit("OFAC"))


distinct_parties_profile_df.writeTo("bronze.distinct_parties") \
    .createOrReplace()

id_reg_documents_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "IDRegDocument") \
    .schema(id_reg_documents_schema) \
    .load(distinct_party_xml)

id_reg_documents_df = id_reg_documents_df.withColumn("ingest_timestamp", current_ts) \
    .withColumn("source_name", lit("OFAC"))

id_reg_documents_df.writeTo("bronze.identities") \
    .createOrReplace()

# Stop the Spark Session
spark.stop()


