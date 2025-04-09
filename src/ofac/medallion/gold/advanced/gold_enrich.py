import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.ofac.medallion.gold.advanced.udf.alias_extract_udf import AliasGoldUDFs
from src.ofac.utility import load_config


class OFACGoldDataTransformer:
    def __init__(self, spark: SparkSession, input_table: str, output_table: str):
        self.spark = spark
        self.input_table = input_table
        self.output_table = output_table

    def extract_aliases(self, df):
        return df.withColumn("names", AliasGoldUDFs.extract_names(col("aliases_enriched")))

    def extract_addresses(self, df):
        return df.withColumn("addresses", AliasGoldUDFs.extract_addresses(col("features_enriched")))

    def extract_sanction_entries(self, df):
        return df.withColumn("sanctions", AliasGoldUDFs.extract_sanction_measures(col("sanction_entries_enriched")))

    def extract_id_documents(self, df):
        return df.withColumn("identifications", AliasGoldUDFs.extract_identifications(col("profile_id"), col("id_documents_enriched")))

    def extract_additional_features(self, df):
        return df.withColumn("additional-features", AliasGoldUDFs.extract_features(col("features_enriched")))

    def extract_relations(self, df):
        return df.withColumn("relations", AliasGoldUDFs.extract_relations(col("relationships_enriched")))

    def transform(self):
        df = self.spark.read.format("iceberg").table(self.input_table)
        df = self.extract_aliases(df)
        df = self.extract_addresses(df)
        df = self.extract_sanction_entries(df)
        df = self.extract_id_documents(df)
        df = self.extract_additional_features(df)
        df = self.extract_relations(df)


        df.writeTo(self.output_table).createOrReplace()


def main():
    config = load_config()

    # Paths to data
    source_data_base_path = config['source_data_base_path']
    reference_data_base_path = config['reference_data_base_path']
    output_base_path = config['output_base_path']
    warehouse_base_dir = config['warehouse_base_dir']

    spark = SparkSession.builder \
        .appName("OFACGoldDataTransformation") \
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


    input_table = os.environ.get('OFAC_INPUT_TABLE', 'silver.ofac_transformed')
    output_table = os.environ.get('OFAC_OUTPUT_TABLE', 'gold.ofac_gold_data')

    transformer = OFACGoldDataTransformer(spark, input_table, output_table)
    transformer.transform()

    output_table = spark.read.format("iceberg").table(output_table)
    output_table.show(truncate=False)

    ofac_ids = [
        "26345", "20861", "36", "10001", "10004", "30629", "10003", "16829", "33854", "16829",
        "36050", "44758", "8244", "10923", "9346", "18369", "540", "36216", "7160", "7157",
        "10486", "10033", "10612", "22256", "28366", "12754", "9647", "11018", "7209",
        "13004", "13121", "10383", "15007", "44196", "9340", "39017", "15043", "15076", "37477",
        "37638", "39593", "23203", "6931", "15007", "7203", "7743", "15037", "15038", "15904",
        "15039", "15044", "20820", "45065", "30226", "25293", "15007", "20239", "25288", "17283"
    ]

    __generate_data_by_profile_id__(output_base_path, output_table, ofac_ids)


def __generate_data_by_profile_id__(output_base_path, profiles_df, profiles):
    for profile_id in profiles:
        (profiles_df.filter(col("profile_id") == profile_id).select(
            col("profile_id"),
            col("names"),
            #col("aliases_enriched"),
            col("addresses"),
            #col("features_enriched"),
            col("sanctions"),
            col("identifications"),
            #col("id_documents"),
            #col("id_documents_enriched"),
            col("additional-features"),
            col("relations")
        ).write
         .mode("overwrite")
         .json(f"{output_base_path}/ofac_gold/{profile_id}"))

if __name__ == "__main__":
    main()