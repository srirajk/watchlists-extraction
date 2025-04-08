from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, MapType


from src.ofac.medallion.silver.advanced_enrichment.udf.common_udf import CommonUDFs


class FeatureUDFs:

    """
    A class to encapsulate UDFs for feature transformation.
    """

    location_struct_type = StructType([StructField("area", ArrayType(
        StructType([StructField("description", StringType(), True), StructField("value", StringType(), True)])), True),
                              StructField("country", ArrayType(StructType([StructField("country", StringType(), True),
                                                                           StructField("country_relevance",
                                                                                       StringType(), True)])), True),
                              StructField("parts", ArrayType(StructType([StructField("part_type", StringType(), True),
                                                                         StructField("part_value", ArrayType(StructType(
                                                                             [StructField("location_value_script_type",
                                                                                          StringType(), True),
                                                                              StructField("location_value",
                                                                                          StringType(), True),
                                                                              StructField("is_primary", BooleanType(),
                                                                                          True)])), True)])), True)])
    enriched_feature_schema = ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("feature_versions", ArrayType(StructType([
            StructField("reliability", StringType(), True),
            StructField("date_period", ArrayType(CommonUDFs.enriched_date_period_schema), True),
            StructField("versions", ArrayType(StructType([
                StructField("value", StringType(), True),
                StructField("detail_type", StringType(), True),
                StructField("detail_reference", StringType(), True)
            ])), True),
            StructField("locations", ArrayType(location_struct_type), True)
        ])), True)
    ]))


    @staticmethod
    @udf(returnType=enriched_feature_schema)
    def transform_feature(features):
        """
        Transform the feature column using a UDF.
        :param feature_column: The feature column to be transformed.
        :return: Transformed feature column.
        """
        if features is None:
            return None

        return_features = []
        for feature in features:
            # print(f"Processing feature: {feature}")  # Placeholder for actual transformation logic
            feature_type_value_dict = feature.feature_type_value
            feature_type = feature_type_value_dict["_VALUE"]
            ef = {
                "type": feature_type,
            }

            feature_versions = feature.feature_versions
            efv_arr = []
            for feature_version in feature_versions:
                efv = {}  # enriched feature version
                reliability = feature_version.reliability_value
                efv["reliability"] = reliability

                # enrich date_period
                if feature_version.date_period is not None:
                    dp_arr = []
                    for dp in feature_version.date_period:
                        dp_arr.append(CommonUDFs.enrich_date_period(dp))
                    efv["date_period"] = dp_arr

                # enrich version
                if feature_version.versions is not None:
                    v_arr = []
                    for v in feature_version.versions:
                        v_arr.append(FeatureUDFs.__enrich_version__(v))
                    efv["versions"] = v_arr

                # enrich locations

                if feature_version.locations is not None and len(feature_version.locations) > 0:
                    efvl_arr = FeatureUDFs.__enrich_locations__(feature_version.locations)
                    efv["locations"] = efvl_arr

                efv_arr.append(efv)
            ef["feature_versions"] = efv_arr
            return_features.append(ef)

        #print(f"Enriched_Features :: {return_features}")
        return return_features  # Placeholder for actual transformation logic

    @staticmethod
    def __enrich_version__(version):
        response = {}
        if version.value is not None:
            response["value"] = version.value
        response["detail_type"] = version.detail_type
        response["detail_reference"] = version.detail_reference
        return response

    @staticmethod
    def __enrich_locations__(locations):
        locations_arr = []
        for location in locations:
            enriched_location = FeatureUDFs.enrich_location(location)
            locations_arr.append(enriched_location)
        return locations_arr

    @staticmethod
    def enrich_location(location):
        #print(f"Processing location: {location}")  # Placeholder for actual location enrichment logic

        # Location Area Enrichment
        location_area_arr = FeatureUDFs.__enrich_location_area__(location.location_area)

        # Location Country Enrichment
        location_country_arr = FeatureUDFs.__enrich_location_country__(location.location_country)

        # Location Parts Enrichment
        location_parts_arr = FeatureUDFs.__enrich_location_parts__(location.location_parts)

        # Check if all are None
        if location_area_arr is None and location_country_arr is None and location_parts_arr is None:
            return None

        return {
            "area": location_area_arr,
            "country": location_country_arr,
            "parts": location_parts_arr
        }

    @staticmethod
    def __enrich_location_area__(location_area):
        if location_area is None:
            return None
        location_area_arr = []
        for location_area_item in location_area:
            area_code = location_area_item.area_code
            area_code_description = area_code["_Description"]
            location_area_arr.append({
                "description": area_code_description,
                "value": area_code["_VALUE"],
            })
        return location_area_arr

    @staticmethod
    def __enrich_location_country__(location_country):
        if location_country is None:
            return None
        location_country_arr = []
        for location_country_item in location_country:
            country = location_country_item.country
            country_value = country["_VALUE"]
            country_relevance = location_country_item.country_relevance
            location_country_arr.append({
                "country": country_value,
                "country_relevance": country_relevance
            })
        return location_country_arr

    @staticmethod
    def __enrich_location_parts__(location_parts):
        if location_parts is None:
            return None
        location_parts_arr = []
        for location_part in location_parts:
            part_type = location_part.location_part_type
            value = []
            for part in location_part.parts:
                p = {}
                if part.location_value_type:
                    p["location_value_script_type"] = part.location_value_type
                p["location_value"] = part.location_value
                p["is_primary"] = part.is_primary
                value.append(p)
            location_parts_arr.append({
                "part_type": part_type,
                "part_value": value
            })
        return location_parts_arr

    @staticmethod
    def __enrich_locations__old__(locations):
        # locations = feature_version.locations
        locations_arr = []
        for location in locations:
            # Location Area Enrichment
            location_area = location.location_area
            location_area_arr = []
            for location_area_item in location_area:
                area_code = location_area_item.area_code
                area_code_description = area_code["_Description"]
                location_area_arr.append({
                    "description": area_code_description,
                    "value": area_code["_VALUE"],
                })

            # Location Country Enrichment
            location_country = location.location_country
            location_country_arr = []
            for location_country_item in location_country:
                country = location_country_item.country
                country_value = country["_VALUE"]
                country_relevance = location_country_item.country_relevance
                location_country_arr.append({
                    "country": country_value,
                    "country_relevance": country_relevance
                })

            location_parts = location.location_parts
            location_parts_arr = []
            for location_part in location_parts:
                part_type = location_part.location_part_type
                value = []
                for part in location_part.parts:
                    p = {}
                    if part.location_value_type:
                        p["location_value_script_type"] = part.location_value_type
                    p["location_value"] = part.location_value
                    p["is_primary"] = part.is_primary
                    value.append(p)
                location_parts_arr.append({
                    "part_type": part_type,
                    "part_value": value
                })

            locations_arr.append({
                "area": location_area_arr,
                "country": location_country_arr,
                "parts": location_parts_arr
            })
        return locations_arr



