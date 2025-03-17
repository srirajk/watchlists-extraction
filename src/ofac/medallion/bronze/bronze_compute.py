import json
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit, date_format, date_trunc
from pyspark.sql.utils import AnalysisException

from src.ofac.schemas import distinct_party_schema, id_reg_documents_schema, location_schema, sanctions_entry_schema, \
    profile_relation_schema
from src.ofac.utility import load_config

def create_spark_session():
    config = load_config()
    warehouse_base_dir = config['warehouse_base_dir']
    
    return (SparkSession.builder
            .appName("ofac_bronze_compute")
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.databricks:spark-xml_2.12:0.18.0')
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", f"{warehouse_base_dir}/catalog")
            .config("spark.local.dir", f"{warehouse_base_dir}/tmp")
            .config("spark.sql.warehouse.dir", f"{warehouse_base_dir}/data")
            .config("spark.sql.defaultCatalog", "local")
            .getOrCreate())

def create_or_append(spark ,df, table_name):
    try:
        # Try to read the existing table
        spark.table(table_name)
        # If successful, append to the existing table
        df.writeTo(table_name) \
            .option("mergeSchema", "true") \
            .append()
    except AnalysisException:
        # If the table doesn't exist, create it
        df.writeTo(table_name) \
            .option("mergeSchema", "true") \
            .createOrReplace()

def process_bronze_layer(spark, config, input_file, current_ts):
    source_data_base_path = config['source_data_base_path']
    distinct_party_xml = source_data_base_path + input_file
    
    # Create bronze database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


    
    # Process DistinctParty data
    distinct_parties_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "DistinctParty") \
        .schema(distinct_party_schema) \
        .load(distinct_party_xml)
    
    distinct_parties_profile_df = distinct_parties_df.select(
        col("_FixedRef"),
        col("_DeltaAction"),
        col("Comment"),
        explode(col("Profile")).alias("Profile")
    )
    
    # Add extraction timestamp and source name
    distinct_parties_profile_df = distinct_parties_profile_df.withColumn("extraction_timestamp", lit(current_ts)) \
        .withColumn("source_name", lit("OFAC")) \
        .withColumn("input_file", lit(input_file))
    
    # Create or append to bronze.distinct_parties table
    create_or_append(spark, distinct_parties_profile_df, "bronze.distinct_parties")
    
    # Process IDRegDocument data
    id_reg_documents_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "IDRegDocument") \
        .schema(id_reg_documents_schema) \
        .load(distinct_party_xml)
    
    # Add extraction timestamp and source name
    id_reg_documents_df = id_reg_documents_df.withColumn("extraction_timestamp", lit(current_ts)) \
        .withColumn("source_name", lit("OFAC")) \
        .withColumn("input_file", lit(input_file))
    
    # Create or append to bronze.identities table
    create_or_append(spark, id_reg_documents_df, "bronze.identities")
    


    # Process Locations data
    locations_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "Location") \
        .schema(location_schema) \
        .load(distinct_party_xml)

    locations_df.printSchema()

    # Add extraction timestamp and source name
    locations_df = locations_df.withColumn("extraction_timestamp", lit(current_ts)) \
        .withColumn("source_name", lit("OFAC")) \
        .withColumn("input_file", lit(input_file))

    create_or_append(spark, locations_df, "bronze.locations")


    # Process SanctionsEntry data
    sanction_entries_raw_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "SanctionsEntry") \
        .schema(sanctions_entry_schema) \
        .load(distinct_party_xml)

    # Add extraction timestamp and source name
    sanction_entries_raw_df = sanction_entries_raw_df.withColumn("extraction_timestamp", lit(current_ts)) \
        .withColumn("source_name", lit("OFAC")) \
        .withColumn("input_file", lit(input_file))

    # Create or append to bronze.sanctions_entries table
    create_or_append(spark, sanction_entries_raw_df, "bronze.sanctions_entries")

    # Process Profile Relationships data
    profile_relationships_raw_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "ProfileRelationship") \
        .schema(profile_relation_schema) \
        .load(distinct_party_xml)

    # Add extraction timestamp and source name
    profile_relationships_raw_df = profile_relationships_raw_df.withColumn("extraction_timestamp", lit(current_ts)) \
        .withColumn("source_name", lit("OFAC")) \
        .withColumn("input_file", lit(input_file))

    # Create or append to bronze.sanctions_entries table
    create_or_append(spark, profile_relationships_raw_df, "bronze.profile_relationships")



def main():
    if len(sys.argv) != 2:
        print("Usage: python bronze_compute.py <input_file_name>")
        sys.exit(1)

    input_file = sys.argv[1]
    spark = create_spark_session()
    config = load_config()
    #current_ts = date_format(date_trunc('minute', current_timestamp()), 'yyyy-MM-dd HH:mm:00')
    current_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:00')
    try:
        process_bronze_layer(spark, config, input_file, current_ts)
        print(f"Bronze layer processing completed successfully for file: {input_file} with extraction timestamp: {current_ts}!")
    except Exception as e:
        print(f"Error processing Bronze layer for file {input_file}: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()