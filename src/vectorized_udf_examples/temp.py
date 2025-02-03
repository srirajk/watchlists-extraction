import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, collect_list, udf, explode, lit, md5, concat, when
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType

def create_spark_session():
    return SparkSession.builder \
        .appName("OFAC Data Merge") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def group_data_by_profile(df):
    return df.groupBy("app_profile_id").agg(
        collect_list(struct(
            df.columns
        )).alias("profile_data")
    )

merge_decision_schema = ArrayType(StructType([
    StructField("action", StringType()),
    StructField("data", StructType([
        StructField("app_profile_id", StringType()),
        StructField("profile_id", StringType()),
        StructField("identity_id", StringType()),
        StructField("alias_id", StringType()),
        StructField("is_active", BooleanType()),
        # Add other fields as needed
    ]))
]))

@udf(merge_decision_schema)
def merge_decision_udf(new_data, existing_data):
    decisions = []

    if existing_data is None:
        # If no existing data, insert all new data
        for record in new_data:
            decisions.append({"action": "insert", "data": {**record, "is_active": True}})
    else:
        existing_identities = {record['identity_id']: record for record in existing_data if record['is_active']}
        for new_record in new_data:
            identity_id = new_record['identity_id']
            if identity_id in existing_identities:
                if new_record != existing_identities[identity_id]:
                    # Deactivate the old record
                    decisions.append({"action": "update", "data": {**existing_identities[identity_id], "is_active": False}})
                    # Insert the new record
                    decisions.append({"action": "insert", "data": {**new_record, "is_active": True}})
                # If the records are the same, do nothing
            else:
                # New alias for existing profile or entirely new record
                decisions.append({"action": "insert", "data": {**new_record, "is_active": True}})

    return decisions

def merge_ofac_data(spark, new_data_df):
    # Generate app_profile_id for new data
    new_data_df = new_data_df.withColumn(
        "app_profile_id",
        when(col("app_profile_id").isNull(),
             concat(lit("APP_"), md5(col("profile_id").cast("string"))))
        .otherwise(col("app_profile_id"))
    )

    # Group new data by app_profile_id
    new_data_grouped = group_data_by_profile(new_data_df)

    # Read existing data from Iceberg table, filter for active records, and group by app_profile_id
    existing_data = spark.read.format("iceberg").table("silver.ofac_enriched").filter(col("is_active") == True)
    existing_data_grouped = group_data_by_profile(existing_data)

    # Join new and existing data
    merged_data = new_data_grouped.join(existing_data_grouped, "app_profile_id", "full_outer")

    # Apply merge decision UDF
    decisions_df = merged_data.select(
        col("app_profile_id"),
        explode(merge_decision_udf(col("new_data_grouped.profile_data"), col("existing_data_grouped.profile_data"))).alias("decision")
    )

    # Split decisions into insert and update dataframes
    inserts_df = decisions_df.filter(col("decision.action") == "insert").select(col("decision.data.*"))
    updates_df = decisions_df.filter(col("decision.action") == "update").select(col("decision.data.*"))

    # Perform merge operations
    inserts_df.writeTo("silver.ofac_enriched").append()

    # For updates, we're setting the is_active flag to False
    updates_df.writeTo("silver.ofac_enriched").overwritePartitions()

def main():

    if len(sys.argv) != 2:
        print("Usage: python silver_compute_wap.py extraction_timestamp")
    sys.exit(1)

    #enrich_ofac_silver_latest_data =



    #bronze_distinct_parties = spark.table("bronze.distinct_parties").filter(col("extraction_timestamp") == extraction_timestamp)
    #bronze_identities = spark.table("bronze.identities").filter(col("extraction_timestamp") == extraction_timestamp)


    # Assuming ofac_enriched_data_df is your new processed data
    ofac_enriched_data_df = spark.read.format("iceberg").table("bronze.ofac_data")  # Replace with actual source

    merge_ofac_data(spark, ofac_enriched_data_df)

    spark.stop()

if __name__ == "__main__":
    main()