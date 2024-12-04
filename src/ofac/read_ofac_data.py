from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, expr
from pyspark.sql.functions import col, explode, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, LongType, BooleanType
import json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.functions import struct


from src.ofac.custom_udfs import enrich_profile_data_udf, transform_document
from src.ofac.schemas import distinct_party_schema, id_reg_documents_schema



# Paths to data
source_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/source_data/ofac/"
reference_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/reference_data/ofac/"

output_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/output/ofac/"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("OFAC_PARSING") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
    .getOrCreate()



distinct_party_xml = source_data_base_path + "sdn_advanced.xml"



reference_values_json = f"{reference_data_base_path}/reference_values_map.json"

# Load reference values from JSON to a dictionary
with open(reference_values_json, 'r') as file:
    reference_data = json.load(file)

# Broadcast each reference dictionary for quick lookup
reference_maps = {key: spark.sparkContext.broadcast(value) for key, value in reference_data.items()}

# Read XML with schema
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
distinct_parties_profile_df.show(truncate=False)
print(f" End Distinct Parties Explode Direct distinct_parties_profile_df")

print(f"Before Alias Explosion Distinct Parties Count : {distinct_parties_df.count()}")

# Register the UDF
enrich_profile_data_udf_invoke = udf(
    lambda profile_row: enrich_profile_data_udf(profile_row),
    ArrayType(
        StructType([
            StructField("profile_id", LongType(), True),
            StructField("identity_id", LongType(), True),
            StructField("alias_type_value", StringType(), True),
            StructField("_Primary", BooleanType(), True),
            StructField("documented_names", ArrayType(MapType(StringType(), StringType())))
        ])
    )
)

distinct_parties_profile_df = (distinct_parties_profile_df
 .withColumn("enriched_party_data_multiple", enrich_profile_data_udf_invoke(col("Profile")))
 .select(explode(col("enriched_party_data_multiple")).alias("enriched_party_data")))

# Flatten the exploded data into columns
distinct_parties_profile_df = distinct_parties_profile_df.select(
    col("enriched_party_data.profile_id").alias("profile_id"),
    col("enriched_party_data.identity_id").alias("identity_id"),
    col("enriched_party_data.alias_type_value").alias("alias_type_value"),
    col("enriched_party_data._Primary").alias("is_primary"),
    col("enriched_party_data.documented_names").alias("documented_names")
)

print(f"After Alias Explosion Distinct Parties Count: {distinct_parties_profile_df.count()}")

#istinct_parties_profile_df.show(truncate=False)


distinct_party_primary_filtered_df = distinct_parties_profile_df.filter(col("is_primary") == True)

print(f"Primary Alias Filtered Distinct Parties Count: {distinct_party_primary_filtered_df.count()}")



id_reg_documents_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "IDRegDocument") \
    .schema(id_reg_documents_schema) \
    .load(distinct_party_xml)


#id_reg_documents_df.write.mode("overwrite").json(f"{output_base_path}/id_reg_documents_df")


# UDF to transform id_reg_documents_df rows
id_reg_documents_transformed_udf = udf(lambda row: transform_document(row), MapType(StringType(), StringType()))

# Apply transformation to id_reg_documents_df
id_reg_documents_transformed_df = id_reg_documents_df.withColumn(
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
    ))  # Pass the entire row to the UDF
)



# Explode and flatten the transformed data
id_documents_df = id_reg_documents_transformed_df.select(
    col("transformed_document.identityId").alias("identity_id"),
    col("transformed_document.document_type").alias("document_type"),
    col("transformed_document.issuing_country").alias("issuing_country"),
    col("transformed_document.id_reg_document_id").alias("id_reg_document_id"),
    col("transformed_document.id_registration_number").alias("id_registration_number"),
    col("transformed_document.document_dates").alias("document_dates")
)

id_documents_df.show(truncate=False)

print(f"Id Documents Raw Dataframe Count: {id_reg_documents_df.count()}")

id_documents_df.write.mode("overwrite").json(f"{output_base_path}/id_documents_df")

id_documents_grouped_by_identity_df = id_documents_df.groupBy("identity_id").agg(
    collect_list(
        struct(
            col("document_type"),
            col("issuing_country"),
            col("id_reg_document_id"),
            col("id_registration_number"),
            col("document_dates")
        )
    ).alias("id_documents"))

id_documents_grouped_by_identity_df.show(truncate=False)

distinct_parties_id_documents_merged_df = distinct_parties_profile_df.join(
    id_documents_grouped_by_identity_df,
    (distinct_parties_profile_df["identity_id"] == id_documents_grouped_by_identity_df["identity_id"]) & (distinct_parties_profile_df["is_primary"] == True), # Join on identity_id and is_primary
    "left"
).drop(id_documents_grouped_by_identity_df["identity_id"]) # Drop the duplicate identity_id column

distinct_parties_id_documents_merged_df.show(truncate=False)

distinct_parties_id_documents_merged_df.write.mode("overwrite").json(f"{output_base_path}/distinct_parties_id_documents_merged_df")

distinct_parties_id_documents_merged_df.createOrReplaceTempView("ofac_silver")

distinct_parties_id_documents_merged_df.where(col("id_documents").isNotNull()).show(truncate=False)


# Filter rows where id_documents is not null
distinct_parties_id_documents_merged_df.where(col("id_documents").isNotNull()).show(truncate=False)

#Filter rows where id_documents.document_type is C.U.R.P.
distinct_parties_id_documents_merged_df.filter(expr("exists(id_documents, x -> x.document_type == 'C.U.R.P.')")).show(truncate=False)

filtered_df = spark.sql("""
    SELECT *
    FROM ofac_silver
    WHERE size(FILTER(id_documents, x -> x.document_type = 'Passport')) > 0
""")

# Show the filtered DataFrame
filtered_df.show(truncate=False)


distinct_document_types_df = spark.sql("""
    SELECT DISTINCT id_doc.document_type
    FROM ofac_silver
    LATERAL VIEW explode(id_documents) exploded_table AS id_doc
    WHERE id_doc.document_type IS NOT NULL
""")

distinct_document_types_df.show(truncate=False)



# distinct_party_primary_identity_filtered_df = distinct_party_primary_filtered_df.join(
#     id_reg_documents_df,
#     distinct_party_primary_filtered_df["identity_id"] == id_reg_documents_df["_IdentityID"],
#     "left"  # Use left join to retain all rows from distinct_party_primary_filtered_df
# )
#
#
#



# distinct_party_non_primary_filtered_df = distinct_parties_profile_df.filter(col("is_primary") == False)
#
# final_df = distinct_party_primary_identity_filtered_df.unionByName(distinct_party_non_primary_filtered_df, allowMissingColumns=True)
#
#
# final_df.show(truncate=False)
#
#
#
#
# final_df.where(col("profile_id") == 11239).show(truncate=False)



# Show the DataFrame
#id_reg_documents_df.show(truncate=False)

id_reg_documents_df.printSchema()

id_reg_documents_df.show(truncate=False)
id_reg_documents_df.where(col("DocumentDate").isNotNull()).show(truncate=False)


id_reg_documents_df_no_si = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "IDRegDocument") \
    .load(distinct_party_xml)

id_reg_documents_df_no_si.printSchema()
id_reg_documents_df_no_si.show(truncate=False)


# Stop the Spark Session
spark.stop()




