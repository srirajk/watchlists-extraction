import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit

from src.ofac.schemas import distinct_party_schema

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


print(f"Distinct Parties Table")
spark.sql("select * from bronze.distinct_parties LIMIT 30").show()


print("Identities Table")
spark.sql("select * from bronze.identities LIMIT 30").show()



distinct_party_raw_sample_df = spark.sql("select * from bronze.distinct_parties LIMIT 30")
pretty_print_spark_df(distinct_party_raw_sample_df)
distinct_party_raw_sample_df.write.mode("overwrite").json(f"{output_base_path}/distinct_party_raw_sample_df")


identities_sample_df = spark.sql("select * from bronze.identities LIMIT 30")
pretty_print_spark_df(identities_sample_df)
identities_sample_df.write.mode("overwrite").json(f"{output_base_path}/identities_sample_df")





# Stop the Spark Session
spark.stop()


