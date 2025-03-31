from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

from src.ofac.medallion.schemas.common_schema import return_date_period_schema

# Define the schema for document_dates
document_date_schema = StructType([
    StructField("document_date_type", StringType(), True),
    StructField("date_period", return_date_period_schema, True)
])

# Define the main schema for the UDF output
id_document_schema = StructType([
    StructField("identity_id", LongType(), True),
    StructField("document_type", StringType(), True),
    StructField("issuing_country", StringType(), True),
    StructField("id_reg_document_id", LongType(), True),
    StructField("id_registration_number", StringType(), True),
    StructField("document_dates", ArrayType(document_date_schema), True),
    StructField("location_id", LongType(), True),
    StructField("validity_id", LongType(), True)
])
