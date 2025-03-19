import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit, size, expr

from src.ofac.custom_udfs import get_relation_quality, get_relation_type, parse_date_period_udf
from src.ofac.schemas import distinct_party_schema

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

ofac_silver = spark.read.format("iceberg").table("silver.ofac_enriched.branch_20250317T123900")



profiles = [39017, 9340, 23203, 6931, 15007, 7203, 7743, 15037, 15038, 26345, 22256, 36216]
for profile_id in profiles:
    ofac_silver.filter(f"profile_id = {profile_id}").write.mode("overwrite").json(f"{output_base_path}/profiles/profile_df_{profile_id}")

"""



profile_relationships = spark.read.format("iceberg").table("bronze.profile_relationships")

profile_relationships.printSchema()

profile_relationships = profile_relationships.select(
    col("_From-ProfileID").alias("from_profile_id"),
    col("_To-ProfileID").alias("to_profile_id"),
    col("_RelationTypeID").alias("relation_type_id"),
    col("_RelationQualityID").alias("relation_quality_id"),
    col("_SanctionsEntryID").alias("sanctions_entry_id"),
    col("_Former").alias("former"),
    col("Comment").alias("comment"),
    col("DatePeriod").alias("date_period_new"),
    col("IDRegDocumentReference").alias("id_document_reference"),
    get_relation_quality(col("_RelationQualityID")).alias("relation_quality"),
    get_relation_type(col("_RelationTypeID")).alias("relation_type"),
    parse_date_period_udf(col("DatePeriod")).alias("date_period")
)


# Group the profile_relationships by to_profile_id
from pyspark.sql.functions import collect_list, struct

# Select all columns except the grouping column for the struct
value_columns = [c for c in profile_relationships.columns if c != "from_profile_id"]

grouped_relationships = profile_relationships.groupBy("from_profile_id").agg(
    collect_list(struct(*[col(c) for c in value_columns])).alias("relationships")
)

grouped_relationships.filter(size("relationships") > 1).show(truncate=False)

# Show the result
#grouped_relationships.show(truncate=False)

grouped_relationships.filter("from_profile_id = 4632").write.mode("overwrite").json(f"{output_base_path}/profile_relationships_raw_4632")




# call a udf to update the relationship quality and relation type






ofac_silver = spark.read.format("iceberg").table("silver.ofac_enriched")

ofac_silver.printSchema()

filtered_df = ofac_silver.filter(
    (col("feature_updated").isNotNull()) &
    (size(col("feature_updated")) > 0) &
    expr("exists(feature_updated, f -> "  # Loop over `feature_updated`
         "f.feature_versions IS NOT NULL AND size(f.feature_versions) > 0 AND "
         "exists(f.feature_versions, fv -> "  # Loop over `feature_versions`
         "fv.versions IS NOT NULL AND size(fv.versions) > 0))")
)


filtered_df.show(truncate=False)


distinct_detail_types = ofac_silver.selectExpr("explode(feature_updated) as feature") \
    .selectExpr("explode(feature.feature_versions) as feature_version") \
    .selectExpr("explode(feature_version.versions) as version") \
    .select("version.detail_type") \
    .distinct()

distinct_detail_types.show(truncate=False)


from pyspark.sql.functions import col, explode

filtered_df = (ofac_silver
               .withColumn("feature", explode(col("feature_updated")))  # Explode feature_updated array
               .withColumn("feature_version", explode(col("feature.feature_versions")))  # Explode feature_versions array
               .withColumn("version", explode(col("feature_version.versions")))  # Explode versions array
               .filter(col("version.detail_type") == "COUNTRY")  # Filter where detail_type is "country"
               .select("identity_id", "profile_id", "feature.*", "feature_version.*", "version.*")  # Select necessary columns
               )

filtered_df.show(truncate=False)

"""
#spark.sql(''' select * from silver.ofac_enriched where profile_id = 9647 ''').write.mode("overwrite").json(f"{output_base_path}/profile_df_9647")
#spark.sql(''' select * from silver.ofac_enriched where profile_id = 16829 ''').write.mode("overwrite").json(f"{output_base_path}/profile_df_16829")