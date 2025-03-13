import copy
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, struct, collect_list, udf, explode, lit, md5, concat, when, to_json, concat_ws, \
    monotonically_increasing_id, size, date_format, date_trunc, explode_outer, array
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType, LongType, MapType

from src.ofac.custom_udfs import enrich_profile_data_udf, transform_document, enrich_location, enrich_features, \
    deep_asdict, enrich_sanction_entries
from src.ofac.schemas import feature_schema, enriched_feature_schema, enrich_sanction_entries_schema

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
    StructField("feature_updated_hash", StringType(), nullable=True),
    StructField("feature_updated", enriched_feature_schema, nullable=True),
    StructField("documents_hash", StringType(), nullable=True),

    StructField("sanctions_entries_hash", StringType(), nullable=True),
    StructField("sanction_entries", ArrayType(enrich_sanction_entries_schema), nullable=True),

    StructField("version", LongType(), nullable=True),  # Added for old data
    StructField("active_flag", StringType(), nullable=True),  # Added for old data
    StructField("updated_at", StringType(), True),  # Added this field
    StructField("end_date", StringType(), True)  # Added this field
])



def get_sanction_entries_by_profile_id(sanction_entries_df):
    enriched_sanctions_entries_df = sanction_entries_df.withColumn("enriched_sanction_entries",
                                                                   enrich_sanction_entries(
                                                                       struct(*sanction_entries_df.columns)))
    #enriched_sanctions_entries_df.show(truncate=False)
    enriched_sanctions_entries_df.filter(col("enriched_sanction_entries.profile_id") == 36).write.mode("overwrite").json(f"{output_base_path}/sanction_entries_df_36")
    enriched_sanctions_entries_df.printSchema()
    enriched_sanctions_entries_df.show(truncate=False, vertical=True)

    print()
    print("************")
    print("************")
    enriched_sanctions_entries_df = enriched_sanctions_entries_df.select(
        col("_ProfileID").alias("profile_id"),
        col("enriched_sanction_entries").alias("sanction_entry"),
    )

    # group by profile_id and collect sanction_entries
    enriched_sanctions_entries_df = enriched_sanctions_entries_df.groupBy("profile_id").agg(
        collect_list("sanction_entry").alias("sanction_entries"))

    #enriched_sanctions_entries_df.printSchema()

    enriched_sanctions_entries_df = enriched_sanctions_entries_df.withColumn("sanctions_entries_hash", md5(to_json(col("sanction_entries"))))

    #enriched_sanctions_entries_df.filter(size(col("sanction_entries")) > 1).select(col("profile_id")).show(truncate=False)


    #enriched_sanctions_entries_df.filter(col("profile_id") == 9647).write.mode("overwrite").json(f"{output_base_path}/enriched_sanction_entries_df_9647")
    return enriched_sanctions_entries_df


def get_locations_by_location_ref_id(spark, locations_df):

   # locations_df = locations_df.filter(col("IDRegDocumentReference").isNotNull())

    process_locations_udf = udf(lambda row: enrich_location(row),
                                StructType([
                                    StructField("location_id", LongType(), True),
                                    StructField("feature_version_refs", ArrayType(LongType()), True),
                                    StructField("id_document_refs", ArrayType(LongType()), True),
                                    StructField("location_area", ArrayType(
                                        StructType([
                                            StructField("area_code", MapType(StringType(), StringType()), True),
                                            StructField("area_code_type", StringType(), True)
                                        ])
                                    ), True),
                                    StructField("location_country", ArrayType(
                                        StructType([
                                            StructField("country", MapType(StringType(), StringType()), True),
                                            StructField("country_relevance", StringType(), True)
                                        ])
                                    ), True),
                                    StructField("location_parts", ArrayType(
                                        StructType([
                                            StructField("location_part_type_id", LongType(), True),
                                            StructField("location_part_type", StringType(), True),
                                            StructField("parts", ArrayType(
                                                StructType([
                                                    StructField("location_party_type_value_id", LongType(), True),
                                                    StructField("location_party_type_value", StringType(), True),
                                                    StructField("location_part_value_status_id", LongType(), True),
                                                    StructField("location_part_value_status", StringType(), True),
                                                    StructField("location_value", StringType(), True),
                                                    StructField("is_primary", BooleanType(), True)
                                                ])
                                            ), True)
                                        ])
                                    ), True),
                                ])
                                )

    locations_enriched_df = locations_df.withColumn("enriched_location",
                                                    process_locations_udf(struct(*locations_df.columns)))

    locations_enriched_df = locations_enriched_df.select(
        col("enriched_location.location_id").alias("location_id"),
        col("enriched_location.feature_version_refs").alias("feature_version_refs"),
        col("enriched_location.id_document_refs").alias("id_document_refs"),
        col("enriched_location.location_area").alias("location_area"),
        col("enriched_location.location_country").alias("location_country"),
        col("enriched_location.location_parts").alias("location_parts"),
    )

    return locations_enriched_df


from pyspark.sql.functions import col, explode, struct, collect_list, when, lit, first

def enrich_profiles_with_locations(spark, profiles_df, locations_df):
    """
    Enhances profiles_df with location information from locations_df
    while preserving the original `feature` column and ensuring nested feature updates
    only apply to primary records.
    """

    # print("Before merge")
    # profiles_df.filter(col("profile_id") == 735).show(truncate=False, vertical=True)

    # Step 1: Preserve the original `feature` column before transformations
    profiles_df = profiles_df.withColumn("original_feature", col("feature"))

    # Step 2: Separate primary and non-primary records
    primary_profiles_df = profiles_df.filter(col("is_primary") == True)
    non_primary_profiles_df = profiles_df.filter(col("is_primary") == False)

    # Step 3: Explode feature array (each feature gets its own row)
    exploded_profiles_df = primary_profiles_df.withColumn("feature", explode(col("feature")))

    # Step 4: Extract feature_id since we need to group by it later
    exploded_profiles_df = exploded_profiles_df.withColumn("feature_id", col("feature.feature_id"))

    # Step 5: Explode feature_versions array (each feature_version gets its own row)
    exploded_profiles_df = exploded_profiles_df.withColumn("feature_version", explode(col("feature.feature_versions")))

    # Step 6: Extract feature_version_id
    exploded_profiles_df = exploded_profiles_df.withColumn("feature_version_id", col("feature_version.version_id"))

    # Step 7: Explode locations array (extract location_id for joining)
    exploded_profiles_df = exploded_profiles_df.withColumn(
        "location_id",
        explode(when(col("feature_version.locations").isNotNull(), col("feature_version.locations")).otherwise(array()))
    )

    # Step 8: Perform left join to bring location data
    joined_df = exploded_profiles_df.join(locations_df, on=["location_id"], how="left")

    #joined_df.filter(col("profile_id") == 735).show(truncate=False, vertical=True)

    # Step 9: Aggregate locations per feature_version_id
    feature_version_grouped = joined_df.groupBy("profile_id", "feature_id", "feature_version_id").agg(
        collect_list(
            struct(
                col("location_id"),
                col("location_area"),
                col("location_country"),
                col("location_parts")
            )
        ).alias("locations")
    )

    # Step 10: Merge enriched locations back into feature_versions
    feature_versions_enriched_df = (
        exploded_profiles_df
        .join(feature_version_grouped, ["profile_id", "feature_id", "feature_version_id"], how="left")
        .groupBy("profile_id", "feature_id")
        .agg(
            collect_list(
                struct(
                    col("feature_version.reliability_id"),
                    col("feature_version.reliability_value"),
                    col("feature_version.version_id"),
                    col("feature_version.versions"),
                    col("locations")  # Assign enriched locations
                )
            ).alias("feature_versions")
        )
    )

    # Step 11: Merge enriched `feature_versions` back into `feature`
    feature_enriched_df = (
        feature_versions_enriched_df
        .groupBy("profile_id")  # Group all features under each profile
        .agg(
            collect_list(
                struct(
                    col("feature_id"),
                    col("feature_versions")  # Enriched feature_versions with locations
                )
            ).alias("feature_enriched")  # Reconstruct the `feature` array
        )
    )

    # Step 12: Merge `feature` **only into primary records**
    enriched_primary_df = primary_profiles_df.join(feature_enriched_df, on=["profile_id"], how="left")

    # Step 13: Restore non-primary records **without modification**
    enriched_df = enriched_primary_df.unionByName(non_primary_profiles_df, allowMissingColumns=True)

    # Step 14: Apply the UDF to enrich the original feature column
    final_df = enriched_df.withColumn(
        "feature_updated",
        enrich_features(col("original_feature"), col("feature_enriched"), col("is_primary"))
    ).drop("feature_enriched", "original_feature", "feature")  # Drop temporary columns

    final_df = final_df.withColumn("feature_updated_hash",
                       when(col("feature_updated").isNotNull(),
                            md5(to_json(col("feature_updated"))))
                       .otherwise(lit(None)))
    return final_df


def enrich_profiles_with_sanction_entries(spark, profiles_df, sanction_entries_df):

    primary_profiles_df = profiles_df.filter(col("is_primary") == True)
    non_primary_profiles_df = profiles_df.filter(col("is_primary") == False)

    joined_df = primary_profiles_df.join(sanction_entries_df, on=["profile_id"], how="left")

    print("****After Joining the DF with the sanctions df****")
    joined_df.filter(col("profile_id") == 9647).write.mode("overwrite").json(f"{output_base_path}/joined_df_9647")

    return joined_df.unionByName(non_primary_profiles_df, allowMissingColumns=True)


def enrich_current_data_extraction(spark, extraction_timestamp):

    locations_df = spark.read.format("iceberg").table("bronze.locations").filter(
        col("extraction_timestamp") == extraction_timestamp)

    locations_df = get_locations_by_location_ref_id(spark, locations_df)

    locations_df.show(truncate=False)
    locations_df.printSchema()

    sanction_entries_df = spark.read.format("iceberg").table("bronze.sanctions_entries").filter(
        col("extraction_timestamp") == extraction_timestamp)

    sanction_entries_df = get_sanction_entries_by_profile_id(sanction_entries_df)


    distinct_parties_df = spark.read.format("iceberg").table("bronze.distinct_parties").filter(
        col("extraction_timestamp") == extraction_timestamp)
    #distinct_parties_df.show(truncate=False)
    distinct_parties_enriched_df = get_profile_df(spark, distinct_parties_df)

    distinct_parties_enriched_df.show(truncate=False)

    distinct_parties_enriched_df.printSchema()

    distinct_parties_with_locations_enriched_df = enrich_profiles_with_locations(spark, distinct_parties_enriched_df, locations_df)

    distinct_parties_full_df = enrich_profiles_with_sanction_entries(spark, distinct_parties_with_locations_enriched_df, sanction_entries_df)

    identities_df = spark.read.format("iceberg").table("bronze.identities").filter(
        col("extraction_timestamp") == extraction_timestamp)

    id_documents_grouped_by_identity_df = get_id_reg_documents_df(spark, identities_df, locations_df)

    # Merge distinct_parties_with_locations_enriched_df and id_documents_grouped_by_identity_df
    ofac_enriched_data_df = distinct_parties_full_df.join(
        id_documents_grouped_by_identity_df,
        (distinct_parties_full_df["identity_id"] == id_documents_grouped_by_identity_df["identity_id"]) & (
                distinct_parties_full_df["is_primary"] == True),  # Join on identity_id and is_primary
        "left"
    ).drop(id_documents_grouped_by_identity_df["identity_id"])  # Drop the duplicate identity_id column

    return ofac_enriched_data_df


def get_id_reg_documents_df(spark, identities_df, locations_df):

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
            col("IssuingAuthority"),
            col("_IssuedIn-LocationID")
        ))
    )

    # Explode and flatten the transformed data
    id_reg_documents_enriched_df = id_reg_documents_enriched_df.select(
        col("transformed_document.identityId").alias("identity_id"),
        col("transformed_document.document_type").alias("document_type"),
        col("transformed_document.issuing_country").alias("issuing_country"),
        col("transformed_document.id_reg_document_id").alias("id_reg_document_id"),
        col("transformed_document.id_registration_number").alias("id_registration_number"),
        col("transformed_document.document_dates").alias("document_dates"),
        col("transformed_document.location_id").alias("issued_in_location_id")
    )

    # Join with Location df to enrich the issued_in_location_id
    id_reg_documents_enriched_df = id_reg_documents_enriched_df.join(
        locations_df,
        id_reg_documents_enriched_df["issued_in_location_id"] == locations_df["location_id"],
        "left"
    ).select(
        id_reg_documents_enriched_df["*"],
        struct(
            col("location_id").alias("id"),
            col("location_area").alias("area"),
            col("location_country").alias("country"),
            col("location_parts").alias("parts")
        ).alias("issued_in_location")
    ).drop("issued_in_location_id")

    id_documents_grouped_by_identity_df = id_reg_documents_enriched_df.groupBy("identity_id").agg(
        collect_list(
            struct(
                col("document_type"),
                col("issuing_country"),
                col("id_reg_document_id"),
                col("id_registration_number"),
                col("document_dates"),
                col("issued_in_location")
            )
        ).alias("id_documents"))


    id_documents_grouped_by_identity_df = id_documents_grouped_by_identity_df.withColumn("documents_hash", md5(to_json(
        col("id_documents"))))

    id_documents_grouped_by_identity_df.show(truncate=False)

    id_documents_grouped_by_identity_df.filter(col("identity_id") == 7157).show(truncate=False)
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
                StructField("feature", feature_schema, True)
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
        col("enriched_party_data.feature").alias("feature")
    )

    distinct_parties_enriched_df.filter(col("profile_id") == 51311).show(truncate=False, vertical=True)

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
    StructField("data", record_schema, True), # Use the record_schema here
    StructField("update_type", StringType(), True),
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
            #new_record = record.asDict()
            new_record = deep_asdict(record)
            new_record["version"] = 1
            new_record["active_flag"] = "Y"
            decisions.append(Row(action="insert", data=new_record, update_type=None))  # Convert Row to dict using asDict()
    else:
        # Existing active alias records (indexed by alias_hash)
        existing_aliases = {rec["alias_hash"]: rec for rec in active_existing_data}

        # Process new records
        for new_record in new_data:
            alias_hash = new_record["alias_hash"]

            if alias_hash in existing_aliases:
                existing_record = existing_aliases[alias_hash]

                if new_record["is_primary"]:
                    documents_updated = new_record["documents_hash"] != existing_record["documents_hash"]
                    features_updated = new_record["feature_updated_hash"] != existing_record["feature_updated_hash"]
                    sanctions_entries_updated = new_record["sanctions_entries_hash"] != existing_record["sanctions_entries_hash"]

                    #if new_record["documents_hash"] != existing_record["documents_hash"]:
                    if documents_updated or features_updated or sanctions_entries_updated:
                        #Deactivate existing primary record
                        #existing_inactive = existing_record.asDict()  # Convert Row to dict
                        existing_inactive = deep_asdict(existing_record)
                        existing_inactive["active_flag"] = "N"
                        decisions.append(Row(action="delete", data=existing_inactive, update_type=None))

                        # Store as dict inside Row

                        # Insert updated version
                        #updated_record = new_record.asDict()  # Convert Row to dict
                        updated_record = deep_asdict(new_record)
                        # TODO add a column to mention if the feature or documents are updated
                        # Instead of two separate flags, create a single update_type column
                        update_type = None
                        if documents_updated and features_updated and sanctions_entries_updated:
                            update_type = "ALL"
                        elif documents_updated:
                            update_type = "DOCUMENTS"
                        elif features_updated:
                            update_type = "FEATURES"
                        elif sanctions_entries_updated:
                            update_type = "SANCTIONS_ENTRIES"

                        updated_record["alias_id"] = existing_record["alias_id"]
                        updated_record["app_profile_id"] = existing_record["app_profile_id"]
                        updated_record["version"] = existing_record["version"] + 1
                        updated_record["active_flag"] = "Y"
                        decisions.append(Row(action="update", data=updated_record, update_type=update_type))

                    # If the primary record exists and has the same documents, do nothing (skip "no_change" records)
            else:
                # New alias → Insert as a new record
                #new_record_dict = new_record.asDict()
                new_record_dict = deep_asdict(new_record)
                new_record_dict["version"] = 1
                new_record_dict["active_flag"] = "Y"
                decisions.append(Row(action="insert", data=new_record_dict, update_type=None))

        # Handle deletions: If an existing active record is no longer in new_data, mark it as deleted
        new_data_alias_hashes = {r["alias_hash"] for r in new_data}
        for existing_record in active_existing_data:
            hash_ = existing_record["alias_hash"]
            if hash_ not in new_data_alias_hashes :
                #existing_inactive = existing_record.asDict()  # Convert Row to dict
                existing_inactive = deep_asdict(existing_record)  # Convert Row to dict
                existing_inactive["active_flag"] = "N"
                decisions.append(Row(action="delete", data=existing_inactive, update_type=None))

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
        col("decision.data.feature_updated_hash").alias("feature_updated_hash"),
        col("decision.data.feature_updated").alias("feature_updated"),
        col("decision.data.sanctions_entries_hash").alias("sanctions_entries_hash"),
        col("decision.data.sanction_entries").alias("sanction_entries"),
        col("decision.update_type").alias("update_type"),
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
    merged_df.printSchema()
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

    extraction_timestamp = "2025-03-13T14:50:00"

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
