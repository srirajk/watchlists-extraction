import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit

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


#spark.sql("describe extended silver.ofac_enriched").show()

#spark.sql("SELECT * FROM silver.ofac_enriched.branch_20250202_164600 LIMIT 50").show()




spark.sql("SELECT * FROM silver.ofac_enriched.refs").show()


#spark.sql("CALL local.system.fast_forward('silver.ofac_enriched', 'main', '20250202_175100')").show()

#spark.sql("SELECT * FROM silver.ofac_enriched.branch_20250202_212500 where profile_id = 36 and is_primary = true and active_flag = 'Y' LIMIT 50").show()

#spark.sql("select * from silver.ofac_enriched  where profile_id = 36").show()

#silver.ofac_enriched.branch_20250202_175100


print("Delta Changes")
spark.sql(''' select * from silver.ofac_enriched_audit_logs_20250205T145500 where profile_id = 173''').show()


#silver.ofac_enriched.branch_20250205T145100

print("branch data")

spark.sql('''
    SELECT 
        profile_id,
        alias_id,
        is_primary,
        active_flag,
        version,
        end_date,
        extraction_timestamp,
        identity_id,
        alias_type_value,
        documented_names,
        app_profile_id,
        alias_hash,
        id_documents
    FROM silver.ofac_enriched.branch_20250205T145500
    where profile_id = 173
''').show(truncate=False)



spark.sql("CALL local.system.fast_forward('silver.ofac_enriched', 'main', '20250205T145500')").show()

print("main table ")

spark.sql('''
    SELECT 
        profile_id,
        alias_id,
        is_primary,
        active_flag,
        version,
        end_date,
        extraction_timestamp,
        identity_id,
        alias_type_value,
        documented_names,
        app_profile_id,
        alias_hash,
        id_documents
    FROM silver.ofac_enriched
    where profile_id = 173
''').show(truncate=False)




# spark.sql("ALTER TABLE silver.ofac_enriched DROP BRANCH 20250203T145800").show()



