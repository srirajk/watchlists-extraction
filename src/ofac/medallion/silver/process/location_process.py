from pyspark.sql.functions import udf, struct, col

from src.ofac.custom_udfs import get_reference_obj_by_key, get_reference_value
from src.ofac.medallion.schemas.location_schema import location_schema
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)


def get_locations_by_location_id_reference(spark, locations_df):

    locations_df = locations_df.withColumn("enriched_location",
                                           enrich_location(struct(*locations_df.columns)))
    locations_df = locations_df.select(
        col("enriched_location.location_id").alias("location_id"),
        col("enriched_location.feature_version_refs").alias("feature_version_refs"),
        col("enriched_location.id_document_refs").alias("id_document_refs"),
        col("enriched_location.location_area").alias("location_area"),
        col("enriched_location.location_country").alias("location_country"),
        col("enriched_location.location_parts").alias("location_parts"),
    )
    return locations_df

@udf(location_schema)
def enrich_location(row):
    location_id = row["_ID"]
    feature_version_refs = row["FeatureVersionReference"]
    feature_version_refs_values = []
    if feature_version_refs:
        for feature_version_ref in feature_version_refs:
            feature_version_refs_values.append(feature_version_ref["_FeatureVersionID"])

    id_document_refs = row["IDRegDocumentReference"]
    id_document_refs_values = []
    if id_document_refs:
        for id_document_ref in id_document_refs:
            id_document_refs_values.append(id_document_ref["_IDRegDocumentID"])

    # Extract LocationAreaCode details
    location_area_codes = row["LocationAreaCode"]
    location_area_codes_values = []
    if location_area_codes:
        for location_area_code in location_area_codes:
            area_code_id = location_area_code["_AreaCodeID"]
            area_code_value = get_reference_obj_by_key("AreaCode", area_code_id)
            area_code_type_id = area_code_value.get("_AreaCodeTypeID")
            area_code_type_value = get_reference_value("AreaCodeType", area_code_type_id)
            location_area = {
                "area_code": area_code_value,
                "area_code_type": area_code_type_value
            }
            location_area_codes_values.append(location_area)

    # Extract LocationCountry details
    location_countries = row["LocationCountry"]
    location_countries_values = []
    if location_countries:
        for location_country in location_countries:
            country_id = location_country["_CountryID"]
            country_value = get_reference_obj_by_key("Country", country_id)
            country_relevance_id = location_country["_CountryRelevanceID"]
            country_relevance_value = get_reference_value("CountryRelevance", country_relevance_id)
            location_country = {
                "country": country_value,
                "country_relevance": country_relevance_value
            }
            location_countries_values.append(location_country)

    # Extract Location Part details
    location_parts = row["LocationPart"]
    location_parts_values = []
    if location_parts:
        for location_part in location_parts:
            location_part_type_id = location_part["_LocPartTypeID"]
            location_part_type = get_reference_value("LocPartType", location_part_type_id)
            parts = []
            for location_part_value in location_part["LocationPartValue"]:
                location_party_type_value_id = location_part_value["_LocPartValueTypeID"]
                location_party_type_value = get_reference_value("LocPartValueType", location_party_type_value_id)
                location_part_value_status_id = location_part_value["_LocPartValueStatusID"]
                location_part_value_status = get_reference_value("LocPartValueStatus", location_part_value_status_id)
                location_value = location_part_value["Value"]
                location_value_type = location_part_value["Comment"]

                part = {
                    "location_party_type_value_id": location_party_type_value_id,
                    "location_party_type_value": location_party_type_value,
                    "location_part_value_status_id": location_part_value_status_id,
                    "location_part_value_status": location_part_value_status,
                    "location_value": location_value,
                    "is_primary": location_part_value["_Primary"],
                    "location_value_type": location_value_type
                }
                parts.append(part)

            location_part = {
                "location_part_type_id": location_part_type_id,
                "location_part_type": location_part_type,
                "parts": parts
            }
            location_parts_values.append(location_part)

    return {
        "location_id": location_id,
        "feature_version_refs": feature_version_refs_values,
        "id_document_refs": id_document_refs_values,
        "location_area": location_area_codes_values,
        "location_country": location_countries_values,
        "location_parts": location_parts_values
    }
