import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, transform, struct, map_from_entries, array, lit, when, expr, create_map, \
    map_keys, explode

from src.ofac.medallion.silver.advanced_enrichment.udf.alias_udf import AliasUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.feature_udf import FeatureUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.id_document_udf import IdDocumentsUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.relationship_udf import RelationshipUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.sanction_entry_udf import SanctionEntryUDfs
from src.ofac.utility import load_config


class OFACEnrichedDataTransformer:
    def __init__(self, spark: SparkSession, input_table: str, output_table: str):
        self.spark = spark
        self.input_table = input_table
        self.output_table = output_table

    def transform_aliases(self, df):
        return df.withColumn("aliases_enriched", AliasUDFs.transform_alias(col("aliases")))#.drop("aliases")

    def transform_features(self, df):
        return df.withColumn("features_enriched", FeatureUDFs.transform_feature(col("feature_updated")))#.drop("feature_updated")

    def transform_sanction_entries(self, df):
        # Implement sanction entries transformation here
        return df.withColumn("sanction_entries_enriched", SanctionEntryUDfs.enrich_sanction_entries(col("sanction_entries")))#.drop("sanction_entries")

    def transform_id_documents(self, df):
        return df.withColumn("id_documents_enriched", IdDocumentsUDFs.enrich_document(col("id_documents")))

    def transform_relationships(self, df):
        # Implement relationships transformation here
        return df.withColumn("relationships_enriched", RelationshipUDFs.enrich_relationships(col("relationships")))

    def transform(self):
        df = self.spark.read.format("iceberg").table(self.input_table)

        #df = df.filter(col("profile_id") == 39017)
        
        df = self.transform_aliases(df)
        df = self.transform_features(df)
        df = self.transform_sanction_entries(df)
        df = self.transform_id_documents(df)
        df = self.transform_relationships(df)

        df.writeTo(self.output_table).createOrReplace()

def main():

    config = load_config()

    # Paths to data
    source_data_base_path = config['source_data_base_path']
    reference_data_base_path = config['reference_data_base_path']
    output_base_path = config['output_base_path']
    warehouse_base_dir = config['warehouse_base_dir']

    spark = SparkSession.builder \
        .appName("OFACDataTransformation") \
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


    input_table = os.environ.get('OFAC_INPUT_TABLE', 'silver.ofac_enriched_advanced')
    output_table = os.environ.get('OFAC_OUTPUT_TABLE', 'silver.ofac_transformed')

    transformer = OFACEnrichedDataTransformer(spark, input_table, output_table)
    transformer.transform()

    # Debug table
    output_table = spark.read.format("iceberg").table(output_table)
    output_table.show(truncate=False)

    ofac_ids = [
        "26345", "20861", "36", "10001", "10004", "30629", "10003", "16829", "33854", "16829",
        "36050", "44758", "8244", "10923", "9346", "18369", "540", "36216", "7160", "7157",
        "10486", "10033", "10612", "22256", "28366", "12754", "9647", "11018", "7209",
        "13004", "13121", "10383", "15007", "44196", "9340", "39017", "15043", "15076", "37477",
        "37638", "39593", "23203", "6931", "15007", "7203", "7743", "15037", "15038", "15904",
        "15039", "15044", "20820", "45065", "30226", "25293", "15007", "20239", "25288"
    ]

    __generate_data_by_profile_id__(output_base_path, output_table, ofac_ids)

    output_table.printSchema()



def __generate_data_by_profile_id__(output_base_path, profiles_df, profiles):
    for profile_id in profiles:
        (profiles_df.filter(col("profile_id") == profile_id).write
         .mode("overwrite")
         .json(f"{output_base_path}/enriched_profiles_df_{profile_id}"))

if __name__ == "__main__":
    main()