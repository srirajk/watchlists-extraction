import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit

from src.ofac.schemas import distinct_party_schema

# Paths to data
source_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/source_data/ofac/"
reference_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/reference_data/ofac/"
output_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/output/ofac/"
warehouse_base_dir = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/iceberg-warehouse"

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


spark.sql("describe extended silver.ofac_enriched").show()


spark.sql("select * from silver.ofac_enriched LIMIT 50").show()

spark.sql("select * from silver.ofac_enriched").printSchema()

# get distinct party types
spark.sql("select distinct party_type from silver.ofac_enriched").show()

# get data where party_type is Entity
spark.sql("select * from silver.ofac_enriched where party_type = 'Transport'  LIMIT 30").show()

# get data where id_documents is not null
spark.sql("select * from silver.ofac_enriched where id_documents is not null").show()


# get distinct document types
spark.sql("""
    SELECT DISTINCT id_doc.document_type
    FROM silver.ofac_enriched
    LATERAL VIEW explode(id_documents) exploded_table AS id_doc
    WHERE id_doc.document_type IS NOT NULL
""").show()

# get document type being passport
passport_documents_df = spark.sql("""
    SELECT *
    FROM silver.ofac_enriched
    WHERE size(FILTER(id_documents, x -> x.document_type = 'Passport')) > 0
""")

passport_documents_df.write.mode("overwrite").json(f"{output_base_path}/passport_documents_df")



# get data where profile_id = 7209
profile_df = spark.sql("select * from silver.ofac_enriched where profile_id = 7209")

profile_df.write.mode("overwrite").json(f"{output_base_path}/profile_df_7209")




# Stop the Spark Session
spark.stop()


