from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, struct, collect_list, md5, lit, concat, monotonically_increasing_id, concat_ws
from pyspark.sql.types import LongType, BooleanType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

from src.ofac.custom_udfs import enrich_profile_data_udf, transform_document

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
spark.sql("DROP TABLE IF EXISTS silver.ofac_enriched PURGE").show()

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

distinct_parties_df = spark.read.format("iceberg").table("bronze.distinct_parties")
distinct_parties_df.show(truncate=False)

identities_df = spark.read.format("iceberg").table("bronze.identities")
identities_df.show(truncate=False)


# Register the UDF
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

distinct_parties_enriched_df = (distinct_parties_df
                               .withColumn("enriched_party_data_multiple", enrich_profile_data_udf_invoke(col("Profile")))
                               .select(explode(col("enriched_party_data_multiple")).alias("enriched_party_data"), "*"))

# Flatten the exploded data into columns
distinct_parties_enriched_df = distinct_parties_enriched_df.select(
    col("enriched_party_data.profile_id").alias("profile_id"),
    col("enriched_party_data.identity_id").alias("identity_id"),
    col("enriched_party_data.alias_type_value").alias("alias_type_value"),
    col("enriched_party_data._Primary").alias("is_primary"),
    col("enriched_party_data.documented_names").alias("documented_names"),
    col("ingest_timestamp"),
    col("source_name"),
    col("_FixedRef"),
    col("enriched_party_data.party_sub_type").alias("party_sub_type"),
    col("enriched_party_data.party_type").alias("party_type"),
)

distinct_parties_enriched_df.show(truncate=False)

distinct_parties_enriched_profile_ids_distinct_df = distinct_parties_enriched_df.select("profile_id").distinct()

generate_app_profile_id_udf = udf(lambda profile_id: f"APP_{md5(str(profile_id).encode()).hexdigest()[:8]}", StringType())


# 1. Generate App Profile IDs
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

# 3. Generate Alias Id for each alias record
# Use a combination of profile_id and an increasing sequence number to generate alias_id

distinct_parties_enriched_df = distinct_parties_enriched_df.withColumn("alias_id",
                                                                       concat_ws("_", col("app_profile_id"), monotonically_increasing_id()))
print("Distinct Parties Enriched with App Profile ID")
distinct_parties_enriched_df.show(truncate=False)


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

id_reg_documents_enriched_df.show(truncate=False)

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

id_documents_grouped_by_identity_df.show(truncate=False)

ofac_enriched_data_df = distinct_parties_enriched_df.join(
    id_documents_grouped_by_identity_df,
    (distinct_parties_enriched_df["identity_id"] == id_documents_grouped_by_identity_df["identity_id"]) & (distinct_parties_enriched_df["is_primary"] == True), # Join on identity_id and is_primary
    "left"
).drop(id_documents_grouped_by_identity_df["identity_id"]) # Drop the duplicate identity_id column

ofac_enriched_data_df.writeTo("silver.ofac_enriched") \
    .createOrReplace()


ofac_enriched_data_df.printSchema()

spark.stop()
