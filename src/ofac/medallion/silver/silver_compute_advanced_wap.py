# main method to be executed
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.ofac.medallion.schemas.ofac_silver_advanced_schema import record_schema
from src.ofac.medallion.silver.process.distinct_party_process import enrich_distinct_party, \
    enrich_distinct_party_with_locations, enrich_distinct_party_with_relationships, \
    enrich_distinct_party_with_sanction_entries, enrich_distinct_party_with_id_documents
from src.ofac.medallion.silver.process.identity_document_process import get_id_reg_documents_df
from src.ofac.medallion.silver.process.location_process import get_locations_by_location_id_reference
from src.ofac.medallion.silver.process.profile_relationship_process import get_profile_relationships_by_profile_id
from src.ofac.medallion.silver.process.sanction_entry_process import get_sanction_entries_by_profile_id
from src.ofac.utility import load_config

config = load_config()

# Paths to data
source_data_base_path = config['source_data_base_path']
reference_data_base_path = config['reference_data_base_path']
output_base_path = config['output_base_path']
warehouse_base_dir = config['warehouse_base_dir']


def enrich_raw_data_extraction(spark, extraction_timestamp):
    print(f"Enriching raw data for extraction timestamp: {extraction_timestamp}")
    locations_df = spark.read.format("iceberg").table("bronze.locations").filter(
        col("extraction_timestamp") == extraction_timestamp)

    locations_df = get_locations_by_location_id_reference(spark, locations_df)

    sanction_entries_df = spark.read.format("iceberg").table("bronze.sanctions_entries").filter(
        col("extraction_timestamp") == extraction_timestamp)

    sanction_entries_df = get_sanction_entries_by_profile_id(spark, sanction_entries_df)

    distinct_parties_df = spark.read.format("iceberg").table("bronze.distinct_parties").filter(
        col("extraction_timestamp") == extraction_timestamp)

    distinct_parties_df = enrich_distinct_party(spark, distinct_parties_df)

    distinct_parties_df = enrich_distinct_party_with_locations(spark, distinct_parties_df, locations_df)

    profile_relationships_df = spark.read.format("iceberg").table("bronze.profile_relationships").filter(
        col("extraction_timestamp") == extraction_timestamp)

    profile_relationships_df = get_profile_relationships_by_profile_id(distinct_parties_df, profile_relationships_df)

    distinct_parties_df = enrich_distinct_party_with_relationships(spark, distinct_parties_df, profile_relationships_df)

    distinct_parties_df = enrich_distinct_party_with_sanction_entries(spark, distinct_parties_df, sanction_entries_df)

    identities_df = spark.read.format("iceberg").table("bronze.identities").filter(
        col("extraction_timestamp") == extraction_timestamp)

    id_documents_grouped_by_identity_df = get_id_reg_documents_df(spark, identities_df, locations_df)

    id_documents_grouped_by_identity_df.filter(col("identity_id") == 6746).write.mode("overwrite").json(f"{output_base_path}/identity_id_6746")

    profiles_df = enrich_distinct_party_with_id_documents(spark, distinct_parties_df, id_documents_grouped_by_identity_df)

    return profiles_df




def __trigger_process__():

    extraction_timestamp = os.getenv("EXTRACTION_TIMESTAMP", "2025-03-17T12:39:00")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ofac_silver_compute") \
        .config('spark.jars.packages',
                'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.databricks:spark-xml_2.12:0.18.0') \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{warehouse_base_dir}/catalog") \
        .config("spark.local.dir", f"{warehouse_base_dir}/tmp") \
        .config("spark.sql.warehouse.dir", f"{warehouse_base_dir}/data") \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()

    table_name = os.getenv("TABLE_NAME", "silver.ofac_enriched_advanced")

    ## TODO: fix this
    #create_table_and_branch(extraction_timestamp, spark, table_name)

    profiles_df = enrich_raw_data_extraction(spark, extraction_timestamp)

    profiles_df.printSchema()

    # For Debugging
    __generate_data_by_profile_id__(profiles_df, ["39017", "7203", "49206"])

    profiles_df.writeTo(f"{table_name}").createOrReplace()


# TODO: fix this
def create_table_and_branch(extraction_timestamp, spark, table_name):
    #create_table_if_not_exists(spark, table_name, record_schema)
    # create branch
    branch_name = extraction_timestamp.replace(" ", "_").replace(":", "").replace("-", "")
    spark.sql(f"ALTER TABLE {table_name} CREATE BRANCH {branch_name}")


def __generate_data_by_profile_id__(profiles_df, profiles):
    for profile_id in profiles:
        (profiles_df.filter(col("profile_id") == profile_id).write
         .mode("overwrite")
         .json(f"{output_base_path}/profiles_df_{profile_id}"))


if __name__ == "__main__":
    __trigger_process__()

