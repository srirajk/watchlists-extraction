from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)

location_schema =  StructType([
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