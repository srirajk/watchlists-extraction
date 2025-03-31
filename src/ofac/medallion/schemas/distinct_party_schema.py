from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)

from src.ofac.medallion.schemas.common_schema import return_date_period_schema

# Schema for version details in feature_versions
version_detail_schema = StructType([
    StructField("value", StringType(), True),
    StructField("detail_type_id", LongType(), True),
    StructField("detail_type", StringType(), True),
    StructField("detail_reference_id", LongType(), True),
    StructField("detail_reference", StringType(), True)
])

feature_version_schema = StructType([
    StructField("reliability_id", LongType(), True),
    StructField("reliability_value", StringType(), True),
    StructField("version_id", LongType(), True),
    StructField("date_period", ArrayType(return_date_period_schema), True),
    StructField("versions", ArrayType(version_detail_schema), True),
    StructField("locations", ArrayType(LongType()), True)
])

feature_schema = ArrayType(StructType([
    StructField("feature_id", LongType(), True),
    StructField("feature_type_id", LongType(), True),
    StructField("feature_type_value", MapType(StringType(), StringType()), True),
    StructField("feature_type_group_id", LongType(), True),
    StructField("feature_type_group_value", StringType(), True),
    StructField("feature_versions", ArrayType(feature_version_schema), True)
]), True)

alias_schema = ArrayType(StructType([
    StructField("is_primary", BooleanType(), True),
    StructField("is_low_quality", BooleanType(), True),
    StructField("identity_id", LongType(), True),
    StructField("alias_type_value", StringType(), True),
    StructField("documented_names", ArrayType(MapType(StringType(), StringType()))),
]))

profile_documented_names_schema = StructType([
    StructField("profile_id", LongType(), True),
    StructField("identity_id", LongType(), True),
    StructField("aliases", alias_schema),
    StructField("party_sub_type", StringType(), True),
    StructField("party_type", StringType(), True),
    StructField("feature", feature_schema, True)
])

enriched_location_schema = StructType([
    StructField("location_id", LongType(), True),
    StructField("location_area", ArrayType(StructType([
        StructField("area_code", MapType(StringType(), StringType()), True),
        StructField("area_code_type", StringType(), True)
    ])), True),
    StructField("location_country", ArrayType(StructType([
        StructField("country", MapType(StringType(), StringType()), True),
        StructField("country_relevance", StringType(), True)
    ])), True),
    StructField("location_parts", ArrayType(StructType([
        StructField("location_part_type_id", LongType(), True),
        StructField("location_part_type", StringType(), True),
        StructField("parts", ArrayType(StructType([
            StructField("location_party_type_value_id", LongType(), True),
            StructField("location_party_type_value", StringType(), True),
            StructField("location_part_value_status_id", LongType(), True),
            StructField("location_part_value_status", StringType(), True),
            StructField("location_value", StringType(), True),
            StructField("is_primary", BooleanType(), True)
        ])), True)
    ])), True)
])


enriched_feature_version_schema = StructType([
    StructField("reliability_id", LongType(), True),
    StructField("reliability_value", StringType(), True),
    StructField("version_id", LongType(), True),
    StructField("date_period", ArrayType(return_date_period_schema), True),
    StructField("versions", ArrayType(StructType([
        StructField("value", StringType(), True),
        StructField("detail_type_id", LongType(), True),
        StructField("detail_type", StringType(), True),
        StructField("detail_reference_id", LongType(), True),
        StructField("detail_reference", StringType(), True),
    ])), True),
    StructField("locations", ArrayType(enriched_location_schema), True)  # This will be enriched
])

# Schema for return feature_versions
enriched_feature_schema = ArrayType(StructType([
    StructField("feature_id", LongType(), True),
    StructField("feature_type_id", LongType(), True),
    StructField("feature_type_value", MapType(StringType(), StringType()), True),
    StructField("feature_type_group_id", LongType(), True),
    StructField("feature_type_group_value", StringType(), True),
    StructField("feature_versions", ArrayType(enriched_feature_version_schema), True)
]), True)
