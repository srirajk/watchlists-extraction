import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, collect_list, udf, explode, lit, md5, concat, when, to_json, concat_ws, \
    monotonically_increasing_id, size, date_format, date_trunc
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType, LongType, MapType

from src.ofac.custom_udfs import enrich_profile_data_udf, transform_document

from src.ofac.utility import load_config

config = load_config()

# Paths to data
source_data_base_path = config['source_data_base_path']
reference_data_base_path = config['reference_data_base_path']
output_base_path = config['output_base_path']
warehouse_base_dir = config['warehouse_base_dir']

from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)

# Define the schema for the DataFrame records
record_schema = StructType([
    StructField("identity_id", LongType(), nullable=True),
    StructField("alias_type_value", StringType(), nullable=True),
    StructField("is_primary", BooleanType(), nullable=True),
    StructField("documented_names", ArrayType(MapType(StringType(), StringType())), nullable=True),
    StructField("extraction_timestamp", StringType(), nullable=True),
    StructField("source_name", StringType(), nullable=True),
    StructField("_FixedRef", StringType(), nullable=True),
    StructField("party_sub_type", StringType(), nullable=True),
    StructField("party_type", StringType(), nullable=True),
    StructField("profile_id", LongType(), nullable=True),
    StructField("app_profile_id", StringType(), nullable=True),
    StructField("alias_id", StringType(), nullable=False),
    StructField("alias_hash", StringType(), nullable=True),
    StructField("id_documents", ArrayType(StructType([
        StructField("document_type", StringType(), nullable=True),
        StructField("issuing_country", StringType(), nullable=True),
        StructField("id_reg_document_id", StringType(), nullable=True),
        StructField("id_registration_number", StringType(), nullable=True),
        StructField("document_dates", StringType(), nullable=True)
    ])), nullable=True),
    StructField("documents_hash", StringType(), nullable=True),
    StructField("version", LongType(), nullable=True),  # Added for old data
    StructField("active_flag", StringType(), nullable=True),  # Added for old data
    StructField("updated_at", StringType(), True),  # Added this field
    StructField("end_date", StringType(), True)  # Added this field
])


def enrich_current_data_extraction(spark, extraction_timestamp):
    distinct_parties_df = spark.read.format("iceberg").table("bronze.distinct_parties").filter(
        col("extraction_timestamp") == extraction_timestamp)
    #distinct_parties_df.show(truncate=False)
    distinct_parties_enriched_df = get_profile_df(spark, distinct_parties_df)

    identities_df = spark.read.format("iceberg").table("bronze.identities").filter(
        col("extraction_timestamp") == extraction_timestamp)
    #identities_df.show(truncate=False)
    id_documents_grouped_by_identity_df = get_id_reg_documents_df(spark, identities_df)

    # Merge distinct_parties_enriched_df and id_documents_grouped_by_identity_df
    ofac_enriched_data_df = distinct_parties_enriched_df.join(
        id_documents_grouped_by_identity_df,
        (distinct_parties_enriched_df["identity_id"] == id_documents_grouped_by_identity_df["identity_id"]) & (
                    distinct_parties_enriched_df["is_primary"] == True),  # Join on identity_id and is_primary
        "left"
    ).drop(id_documents_grouped_by_identity_df["identity_id"])  # Drop the duplicate identity_id column

    return ofac_enriched_data_df



def get_id_reg_documents_df(spark, identities_df):
    # UDF to transform id_reg_documents_df rows
    id_reg_documents_transformed_udf = udf(lambda row: transform_document(row), MapType(StringType(), StringType()))

    # Apply transformation to id_reg_documents_df
    id_reg_documents_enriched_df = identities_df.withColumn(
        "transformed_document",
        id_reg_documents_transformed_udf(struct(
            col("_ID"),
            col("_IDRegDocTypeID"),
            col("_IdentityID"),
            col("_IssuedBy-CountryID"),
            col("DocumentDate"),
            col("Comment"),
            col("IDRegistrationNo"),
            col("IssuingAuthority")
        ))
    )

    # Explode and flatten the transformed data
    id_reg_documents_enriched_df = id_reg_documents_enriched_df.select(
        col("transformed_document.identityId").alias("identity_id"),
        col("transformed_document.document_type").alias("document_type"),
        col("transformed_document.issuing_country").alias("issuing_country"),
        col("transformed_document.id_reg_document_id").alias("id_reg_document_id"),
        col("transformed_document.id_registration_number").alias("id_registration_number"),
        col("transformed_document.document_dates").alias("document_dates")
    )

    id_documents_grouped_by_identity_df = id_reg_documents_enriched_df.groupBy("identity_id").agg(
        collect_list(
            struct(
                col("document_type"),
                col("issuing_country"),
                col("id_reg_document_id"),
                col("id_registration_number"),
                col("document_dates")
            )
        ).alias("id_documents"))

    # id_reg_documents_enriched_df = id_reg_documents_enriched_df.withColumn("documents_hash", md5(to_json(col("id_documents"))))
    #id_documents_grouped_by_identity_df.show(truncate=False)
    id_documents_grouped_by_identity_df = id_documents_grouped_by_identity_df.withColumn("documents_hash", md5(to_json(
        col("id_documents"))))
    #id_documents_grouped_by_identity_df.show(truncate=False)
    return id_documents_grouped_by_identity_df


def get_profile_df(spark, distinct_parties_df):
    enrich_profile_data_udf_invoke = udf(
        lambda profile_row: enrich_profile_data_udf(profile_row),
        ArrayType(
            StructType([
                StructField("profile_id", LongType(), True),
                StructField("identity_id", LongType(), True),
                StructField("alias_type_value", StringType(), True),
                StructField("_Primary", BooleanType(), True),
                StructField("documented_names", ArrayType(MapType(StringType(), StringType()))),
                StructField("party_sub_type", StringType(), True),
                StructField("party_type", StringType(), True),
            ])
        )
    )

    # Flatten the exploded data into columns
    distinct_parties_enriched_df = (distinct_parties_df
                                    .withColumn("enriched_party_data_multiple",
                                                enrich_profile_data_udf_invoke(col("Profile")))
                                    .select(explode(col("enriched_party_data_multiple")).alias("enriched_party_data"),
                                            "*"))

    distinct_parties_enriched_df = distinct_parties_enriched_df.select(
        col("enriched_party_data.profile_id").alias("profile_id"),
        col("enriched_party_data.identity_id").alias("identity_id"),
        col("enriched_party_data.alias_type_value").alias("alias_type_value"),
        col("enriched_party_data._Primary").alias("is_primary"),
        col("enriched_party_data.documented_names").alias("documented_names"),
        col("extraction_timestamp"),
        col("source_name"),
        col("_FixedRef"),
        col("enriched_party_data.party_sub_type").alias("party_sub_type"),
        col("enriched_party_data.party_type").alias("party_type"),
    )

    distinct_parties_enriched_df = distinct_parties_enriched_df.withColumn(
        "documented_names_hash", md5(to_json(col("documented_names")))
    )

    distinct_parties_enriched_df = distinct_parties_enriched_df.withColumn(
        "alias_id", md5(to_json(struct(
            col("profile_id"),
            col("identity_id"),
            col("alias_type_value"),
            col("documented_names_hash")
        )))
    )
    #distinct_parties_enriched_df.show(truncate=False)

    distinct_parties_enriched_profile_ids_distinct_df = distinct_parties_enriched_df.select("profile_id").distinct()


    # Generate App Profile IDs
    distinct_parties_enriched_profile_ids_distinct_df = distinct_parties_enriched_profile_ids_distinct_df.withColumn(
        "app_profile_id", concat(lit("APP_"), md5(col("profile_id").cast("string")))
    )

    print("Distinct Profile IDs with App Profile IDs")
    distinct_parties_enriched_profile_ids_distinct_df.show(truncate=False)

    # 2. Add App_Profile_ID to Exploded Records
    distinct_parties_enriched_df = distinct_parties_enriched_df.join(
        distinct_parties_enriched_profile_ids_distinct_df,
        distinct_parties_enriched_df["profile_id"] == distinct_parties_enriched_profile_ids_distinct_df["profile_id"],
        "left"
    ).drop(distinct_parties_enriched_profile_ids_distinct_df["profile_id"])

    #print("Distinct Parties Enriched with App Profile ID")
    #distinct_parties_enriched_df.show(truncate=False)

    distinct_parties_enriched_df = generate_hash_for_parties(distinct_parties_enriched_df)

    #print("Distinct Parties Enriched with App Profile ID and Alias Hash")
    #distinct_parties_enriched_df.show(truncate=False)

    return distinct_parties_enriched_df


def generate_hash_for_parties(df):
    hash_columns = [
        "profile_id", "identity_id", "alias_type_value", "is_primary",
        "documented_names", "source_name", "_FixedRef",
        "party_sub_type", "party_type"
    ]
    return df.withColumn("alias_hash", md5(to_json(struct(*[col(c) for c in hash_columns]))))


def group_data_by_profile(df):
    # Get all columns except the grouping columns
    value_columns = [c for c in df.columns if c not in ["profile_id", ]]

    df = df.groupBy("profile_id").agg(
        collect_list(struct(*[col(c) for c in value_columns])).alias("alias_data")
    )
    df.show(truncate=False)
    return df


from pyspark.sql.types import ArrayType

# Define the schema for the decision records
decision_schema = StructType([
    StructField("action", StringType(), True),
    StructField("data", record_schema, True)  # Use the record_schema here
])

# Define the schema for the UDF output
merge_decision_schema = ArrayType(decision_schema)

from pyspark.sql.functions import udf

from pyspark.sql import Row

from pyspark.sql import Row
from pyspark.sql.functions import udf

@udf(returnType=merge_decision_schema)
def compare_alias_data(new_data, existing_data):
    """
    Compares new alias data with existing data to determine whether records should be inserted, updated, or deleted.
    Returns a structured list of decisions, ensuring compatibility with the defined schema.
    """
    if new_data is None:
        new_data = []

    decisions = []
    active_existing_data = []
    processed_deletes_aliases = []

    #print(f"New Data: {new_data}")
    #print(f"Existing Data: {existing_data}")

    if existing_data:
        # Filter only active records from existing data
        active_existing_data = [rec for rec in existing_data if rec["active_flag"] == "Y"]

    if not active_existing_data:
        # If no active records exist, insert all new records
        for record in new_data:
            new_record = record.asDict()
            new_record["version"] = 1
            new_record["active_flag"] = "Y"
            decisions.append(Row(action="insert", data=new_record))  # Convert Row to dict using asDict()
    else:
        # Existing active alias records (indexed by alias_hash)
        existing_aliases = {rec["alias_hash"]: rec for rec in active_existing_data}

        # Process new records
        for new_record in new_data:
            alias_hash = new_record["alias_hash"]

            if alias_hash in existing_aliases:
                existing_record = existing_aliases[alias_hash]

                if new_record["is_primary"]:
                    if new_record["documents_hash"] != existing_record["documents_hash"]:
                        #Deactivate existing primary record
                        existing_inactive = existing_record.asDict()  # Convert Row to dict
                        existing_inactive["active_flag"] = "N"
                        decisions.append(Row(action="delete", data=existing_inactive))

                        # Store as dict inside Row

                        # Insert updated version
                        updated_record = new_record.asDict()  # Convert Row to dict
                        updated_record["alias_id"] = existing_record["alias_id"]
                        updated_record["app_profile_id"] = existing_record["app_profile_id"]
                        updated_record["version"] = existing_record["version"] + 1
                        updated_record["active_flag"] = "Y"
                        decisions.append(Row(action="update", data=updated_record))

                    # If the primary record exists and has the same documents, do nothing (skip "no_change" records)
            else:
                # New alias → Insert as a new record
                new_record_dict = new_record.asDict()
                new_record_dict["version"] = 1
                new_record_dict["active_flag"] = "Y"
                decisions.append(Row(action="insert", data=new_record_dict))

        # Handle deletions: If an existing active record is no longer in new_data, mark it as deleted
        new_data_alias_hashes = {r["alias_hash"] for r in new_data}
        for existing_record in active_existing_data:
            hash_ = existing_record["alias_hash"]
            if hash_ not in new_data_alias_hashes :
                existing_inactive = existing_record.asDict()  # Convert Row to dict
                existing_inactive["active_flag"] = "N"
                decisions.append(Row(action="delete", data=existing_inactive))

    #print(f"Final Decisions: {decisions}")
    return decisions





from pyspark.sql.functions import col, explode, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


def merge_ofac_data(spark, new_data_df, table_name, branch_name):
    # Set the WAP branch
    spark.conf.set('spark.wap.branch', branch_name)

    print("New Data")
    new_data_df.show(truncate=False)

    # Group new data by profile_id
    new_data_grouped = group_data_by_profile(new_data_df)

    print("New Data Grouped")
    #new_data_grouped.show(truncate=False)

    # Read existing data from the branch
    existing_data = spark.sql(f"SELECT * FROM {table_name}.branch_{branch_name}")
    existing_data_grouped = group_data_by_profile(existing_data)

    print("Existing Data Grouped")
    #existing_data_grouped.show(truncate=False)

    # Join new and existing data on profile_id
    merged_data = new_data_grouped.alias("new").join(
        existing_data_grouped.alias("existing"),
        "profile_id",
        "full_outer"
    ).select(
        col("profile_id"),
        col("new.alias_data").alias("new_alias_data"),
        col("existing.alias_data").alias("existing_alias_data")
    )

    print("merged data")
    merged_data.show(truncate=False)

    # Apply merge decision UDF
    decisions_df = merged_data.select(
        col("profile_id"),
        explode(compare_alias_data(col("new_alias_data"), col("existing_alias_data"))).alias("decision")
    )
    print("Decisions DF")
    decisions_df.show(truncate=False)

    current_ts = date_format(date_trunc('minute', current_timestamp()), 'yyyy-MM-dd HH:mm:00')

    final_df = decisions_df.select(
        col("profile_id"),
        col("decision.data.identity_id").alias("identity_id"),
        col("decision.data.alias_type_value").alias("alias_type_value"),
        col("decision.data.is_primary").alias("is_primary"),
        col("decision.data.documented_names").alias("documented_names"),
        col("decision.data.extraction_timestamp").alias("extraction_timestamp"),
        col("decision.data.source_name").alias("source_name"),
        col("decision.data._FixedRef").alias("_FixedRef"),
        col("decision.data.party_sub_type").alias("party_sub_type"),
        col("decision.data.party_type").alias("party_type"),
        col("decision.data.app_profile_id").alias("app_profile_id"),
        col("decision.data.alias_id").alias("alias_id"),
        col("decision.data.alias_hash").alias("alias_hash"),
        col("decision.data.id_documents").alias("id_documents"),
        col("decision.data.documents_hash").alias("documents_hash"),
        col("decision.data.version").alias("version"),
        col("decision.data.active_flag").alias("active_flag"),
        col("decision.data.updated_at").alias("updated_at"),
        col("decision.data.end_date").alias("end_date"),
        col("decision.action").alias("action"),# Extract `action` separately
    )

    final_df.show(truncate=False)

    final_df = final_df.withColumn("end_date",
                                   when(col("action") == "delete", lit(current_ts))
                                   .otherwise(col("end_date"))
                                   ).withColumn( "updated_at", lit(current_ts))

    print("Existing DataFrame")
    existing_data.show(truncate=False)

    print("Final DataFrame with all the required changes")
    final_df.show(truncate=False)
    final_df.writeTo(f"silver.ofac_enriched_audit_logs_{branch_name}").createOrReplace()

    final_df.createOrReplaceTempView("source_data")

    merge_sql = f'''
        MERGE INTO {table_name}.branch_{branch_name} AS target
        using source_data as source
        on target.alias_id = source.alias_id  -- Match on alias_id
        and target.active_flag = 'Y'  -- Match only active records in target table
        and target.extraction_timestamp = source.extraction_timestamp  -- Match on extraction_timestamp
        
        WHEN MATCHED AND source.action = 'delete' THEN
            UPDATE SET
            target.active_flag = source.active_flag,
            target.end_date = source.end_date  -- Deactivate the record
        WHEN NOT MATCHED AND source.action IN ('update', 'insert') THEN
          INSERT ({', '.join([field.name for field in record_schema.fields])})
          VALUES ({', '.join([f"source.{field.name}" for field in record_schema.fields])});
    
    '''

    spark.sql(merge_sql)
    print("after merge")
    merged_df = spark.sql(f"SELECT * FROM {table_name}.branch_{branch_name}")
    merged_df.show(truncate=False)
    pass


def create_table_if_not_exists(spark, table_name, schema):
    if not spark.catalog.tableExists(table_name):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("iceberg").saveAsTable(table_name)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists.")


def main():
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

    extraction_timestamp = "2025-02-05T14:55:00"

    # create table if not existing with the schema defined record_schema (scd2 covered schema)
    table_name = "silver.ofac_enriched"
    create_table_if_not_exists(spark, table_name, record_schema)

    # create branch
    branch_name = extraction_timestamp.replace(" ", "_").replace(":", "").replace("-", "")
    spark.sql(f"ALTER TABLE {table_name} CREATE BRANCH {branch_name}")

    # Enrich current data extraction
    enrich_ofac_silver_latest_data = enrich_current_data_extraction(spark, extraction_timestamp)

    enrich_ofac_silver_latest_data.show(truncate=False)

    enrich_ofac_silver_latest_data.printSchema()


    # Merge OFAC data
    merge_ofac_data(spark, enrich_ofac_silver_latest_data, table_name, branch_name)




    print(f"Successfully written data to the table {table_name}.branch_{branch_name}")
    spark.stop()


if __name__ == "__main__":
    main()
