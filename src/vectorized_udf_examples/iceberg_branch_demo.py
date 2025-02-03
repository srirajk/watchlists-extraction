from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

warehouse_base_dir = "/Users/srirajkadimisetty/projects/spark-python-demo/iceberg_warehouse"

raw_data_dir = "/Users/srirajkadimisetty/projects/demo-iceberg/raw_data/yellow_taxi"

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder \
    .appName("IcebergLocalDevelopment") \
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", f"{warehouse_base_dir}/catalog") \
    .config("spark.local.dir", f"{warehouse_base_dir}/tmp") \
    .config("spark.sql.warehouse.dir", f"{warehouse_base_dir}/data") \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()

# Verify Spark session creation
#spark.sql("SHOW DATABASES").show()

spark.sql("DROP TABLE IF EXISTS nyc.taxis PURGE").show()

spark.sql("CREATE DATABASE IF NOT EXISTS nyc")



source_df = spark.read.format("parquet") \
    .load(f"{raw_data_dir}/yellow_tripdata_2023-01.parquet")

df = source_df.withColumn("year", lit(2023)).withColumn("month", lit(1))
df.show()
df.write.partitionBy("year", "month").mode("append").saveAsTable("nyc.taxis")


spark.sql("DESCRIBE EXTENDED nyc.taxis").show()

spark.sql("SELECT year, month, COUNT(*) as count FROM nyc.taxis GROUP BY year, month").show()


# altering the table
spark.sql("ALTER TABLE nyc.taxis SET TBLPROPERTIES ( 'write.wap.enabled'='true' )").show()

branch = "update_data"

# creating a branch
spark.sql("ALTER TABLE nyc.taxis CREATE BRANCH update_data").show()



spark.conf.set('spark.wap.branch', branch)

updated_df = spark.read.format("parquet") \
    .load(f"{raw_data_dir}/yellow_tripdata_2024-01.parquet")
df = updated_df.withColumn("year", lit(2024)).withColumn("month", lit(1))
df.show()
df.write.partitionBy("year", "month") .mode("append").saveAsTable("nyc.taxis")


print(f"******* current branch  {branch} *******")

spark.sql("SELECT year, month, COUNT(*) as count FROM nyc.taxis.branch_update_data GROUP BY year, month").show()


print(f"******* main branch *******")

spark.sql("SELECT year, month, COUNT(*) as count FROM nyc.taxis.branch_main GROUP BY year, month").show()


print(f"******* merging branch {branch} to main *******")


spark.sql("CALL local.system.fast_forward('nyc.taxis', 'main', 'update_data')").show()


print(f"******* main branch *******")

spark.sql("SELECT year, month, COUNT(*) as count FROM nyc.taxis.branch_main GROUP BY year, month").show()



# Load the Iceberg table using the Java API through Spark's Java gateway
table = spark._jvm.org.apache.iceberg.catalog.Catalogs.loadTable(spark._jsparkSession, "local.nyc.taxis")

# Load the table from the catalog
#table = spark.table("local.nyc.taxis")

# Print the branches of the table
print(table.branches())








