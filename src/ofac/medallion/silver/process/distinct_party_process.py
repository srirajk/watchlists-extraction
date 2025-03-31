from pyspark.sql.functions import udf, struct, col, md5, to_json, concat, lit, explode, when, array, collect_list

from src.ofac.custom_udfs import get_reference_value, get_reference_value_by_sub_key, get_reference_obj_by_key, \
    parse_date_period
from src.ofac.medallion.schemas.distinct_party_schema import profile_documented_names_schema, enriched_feature_schema

def enrich_distinct_party(spark, distinct_parties_df):
    distinct_parties_df = distinct_parties_df.withColumn("enriched_profile", enrich_profile_record_udf(col("Profile")))
    distinct_parties_df = distinct_parties_df.select(
        col("extraction_timestamp"),
        col("source_name"),
        col("_FixedRef"),
        col("enriched_profile.profile_id").alias("profile_id"),
        col("enriched_profile.identity_id").alias("identity_id"),
        col("enriched_profile.party_sub_type").alias("party_sub_type"),
        col("enriched_profile.party_type").alias("party_type"),
        col("enriched_profile.feature").alias("feature"),
        col("enriched_profile.aliases").alias("aliases"),
    )
    distinct_parties_df = distinct_parties_df.withColumn("aliases_hash",
                                                         md5(to_json(col("aliases"))))
    distinct_parties_df = distinct_parties_df.withColumn(
        "app_profile_id", concat(lit("APP_"), md5(col("profile_id").cast("string")))
    )
    return distinct_parties_df


def enrich_distinct_party_with_id_documents(spark, distinct_parties_df, id_documents_grouped_by_identity_df):

    return distinct_parties_df.join(
        id_documents_grouped_by_identity_df,
        (distinct_parties_df["identity_id"] == id_documents_grouped_by_identity_df["identity_id"]),  # Join on identity_id and is_primary
        "left"
    ).drop(id_documents_grouped_by_identity_df["identity_id"])  # Drop the duplicate identity_id column



def enrich_distinct_party_with_sanction_entries(spark, distinct_parties_df, sanction_entries_df):
    return distinct_parties_df.join(sanction_entries_df, on=["profile_id"], how="left")

def enrich_distinct_party_with_relationships(spark, distinct_parties_df, profile_relationships_df):
    return distinct_parties_df.join(
        profile_relationships_df,
        distinct_parties_df["profile_id"] == profile_relationships_df["from_profile_id"],
        "left"
    ).drop(profile_relationships_df["from_profile_id"])

def enrich_distinct_party_with_locations(spark, distinct_parties_df, locations_df):
    parties_df = distinct_parties_df.withColumn("original_feature", col("feature"))

    exploded_parties_df = parties_df.withColumn("feature", explode(col("feature")))

    exploded_parties_df = exploded_parties_df.withColumn("feature_id", col("feature.feature_id"))

    exploded_parties_df = exploded_parties_df.withColumn("feature_version", explode(col("feature.feature_versions")))

    exploded_parties_df = exploded_parties_df.withColumn("feature_version_id", col("feature_version.version_id"))

    exploded_parties_df = exploded_parties_df.withColumn(
        "location_id",
        explode(when(col("feature_version.locations").isNotNull(), col("feature_version.locations")).otherwise(array()))
    )

    joined_df = exploded_parties_df.join(locations_df, on=["location_id"], how="left")

    feature_version_grouped = joined_df.groupBy("profile_id", "feature_id", "feature_version_id").agg(
        collect_list(
            struct(
                col("location_id"),
                col("location_area"),
                col("location_country"),
                col("location_parts")
            )
        ).alias("locations")
    )

    feature_versions_enriched_df = (
        exploded_parties_df
        .join(feature_version_grouped, ["profile_id", "feature_id", "feature_version_id"], how="left")
        .groupBy("profile_id", "feature_id")
        .agg(
            collect_list(
                struct(
                    col("feature_version.reliability_id"),
                    col("feature_version.reliability_value"),
                    col("feature_version.version_id"),
                    col("feature_version.versions"),
                    col("locations")  # Assign enriched locations
                )
            ).alias("feature_versions")
        )
    )

    feature_enriched_df = (
        feature_versions_enriched_df
        .groupBy("profile_id")  # Group all features under each profile
        .agg(
            collect_list(
                struct(
                    col("feature_id"),
                    col("feature_versions")  # Enriched feature_versions with locations
                )
            ).alias("feature_enriched")  # Reconstruct the `feature` array
        )
    )

    enriched_df = parties_df.join(feature_enriched_df, on=["profile_id"], how="left")

    final_df = enriched_df.withColumn(
        "feature_updated",
        enrich_features(col("original_feature"), col("feature_enriched"))
    ).drop("feature_enriched", "original_feature", "feature")  # Drop temporary columns

    final_df = final_df.withColumn("feature_updated_hash",
                                   when(col("feature_updated").isNotNull(),
                                        md5(to_json(col("feature_updated"))))
                                   .otherwise(lit(None)))
    return final_df

@udf(profile_documented_names_schema)
def enrich_profile_record_udf(profile_row):
    profile_id = profile_row["_ID"]
    party_sub_type_id = profile_row["_PartySubTypeID"]
    party_sub_type_value = get_reference_value("PartySubType", party_sub_type_id)
    party_type_id = get_reference_value_by_sub_key("PartySubType", party_sub_type_id, "_PartyTypeID")
    party_type_value = get_reference_value("PartyType", party_type_id)
    features = __enrich_features__(profile_row)
    identity_id, aliases = __enrich_aliases__(profile_row)
    enriched_profile_row = {
        "profile_id": profile_id,
        "party_sub_type": party_sub_type_value,
        "party_type": party_type_value,
        "feature": features,
        "aliases": aliases,
        "identity_id": identity_id
    }
    return enriched_profile_row


@udf(enriched_feature_schema)
def enrich_features(original_feature, feature_enriched):
    if not original_feature:
        return []  # Return as is for non-primary records

    enriched_lookup = {
        fe["feature_id"]: {fv["version_id"]: fv["locations"] for fv in fe["feature_versions"]}
        for fe in (feature_enriched or [])  # Handle case where feature_enriched is None
    }

    enriched_features = []
    for feature in original_feature:
        feature_id = feature["feature_id"]
        feature_versions = feature["feature_versions"]

        enriched_feature_versions = []
        for feature_version in feature_versions:
            version_id = feature_version["version_id"]

            # Create a new dictionary copy of the feature_version row
            new_feature_version = dict(feature_version.asDict())  # Convert Row to mutable dictionary

            # Enrich feature_version if locations exist in enriched_lookup
            if feature_id in enriched_lookup and version_id in enriched_lookup[feature_id]:
                new_feature_version["locations"] = enriched_lookup[feature_id][version_id]

            enriched_feature_versions.append(new_feature_version)

        # Add enriched feature back with original structure
        enriched_features.append({
            "feature_id": feature_id,
            "feature_type_id": feature["feature_type_id"],
            "feature_type_value": feature["feature_type_value"],
            "feature_type_group_id": feature["feature_type_group_id"],
            "feature_type_group_value": feature["feature_type_group_value"],
            "feature_versions": enriched_feature_versions
        })

    return enriched_features


def __enrich_aliases__(profile_row):
    name_part_values_with_ref = {}
    if "Identity" in profile_row and profile_row["Identity"]:
        for group in profile_row["Identity"][0]["NamePartGroups"]["MasterNamePartGroup"]:
            group_id = group["NamePartGroup"]["_ID"]
            name_part_type_id = group["NamePartGroup"]["_NamePartTypeID"]
            name_part_values_with_ref[str(group_id)] = get_reference_value("NamePartType", name_part_type_id)
    aliases = []
    identity_id = 0 # Assumption is there is a single IdentityId within the profile
    if "Identity" in profile_row and profile_row["Identity"]:
        for identity in profile_row["Identity"]:  # Loop over all identities
            identity_id = identity["_ID"]
            for alias in identity["Alias"]:
                alias_type_id = alias["_AliasTypeID"]
                alias_type_value = get_reference_value("AliasType", alias_type_id)
                is_primary = alias["_Primary"]
                is_low_quality = alias["_LowQuality"]
                documented_names = []
                for documented_name in alias["DocumentedName"]:
                    documented_name_json = {}
                    for part in documented_name["DocumentedNamePart"]:
                        name_part_value = part["NamePartValue"]
                        group_id = name_part_value["_NamePartGroupID"]
                        mapped_value = name_part_values_with_ref.get(str(group_id), "Unknown")
                        documented_name_json[mapped_value] = name_part_value["_VALUE"]
                        script_id = name_part_value["_ScriptID"]
                        script_value = get_reference_obj_by_key("Script", script_id)
                        script_status_id = name_part_value["_ScriptStatusID"]
                        script_status_value = get_reference_value("ScriptStatus", script_status_id)
                        documented_name_json["Script"] = {
                            "scriptId": script_id,
                            "scriptValue": script_value,
                        }
                        documented_name_json["ScriptStatus"] = {
                            "scriptStatusId": script_status_id,
                            "scriptStatusValue": script_status_value
                        }
                    documented_names.append(documented_name_json)
                aliases.append({
                    "identity_id": identity_id,
                    "alias_type_value": alias_type_value,
                    "is_primary": is_primary,
                    "is_low_quality": is_low_quality,
                    "documented_names": documented_names,
                })
    return identity_id, aliases


def __enrich_features__(profile_row):
    features = []
    if "Feature" in profile_row and profile_row["Feature"]:
        for feature in profile_row["Feature"]:
            feature_id = feature["_ID"]
            feature_type_id = feature["_FeatureTypeID"]
            feature_type_value = get_reference_obj_by_key("FeatureType", feature_type_id)
            feature_type_group_id = feature_type_value.get("_FeatureTypeGroupID", None)
            feature_type_group_value = "Unknown" if feature_type_group_id is None else get_reference_value(
                "FeatureTypeGroup", feature_type_group_id)

            feature_versions = feature["FeatureVersion"]
            # print(feature_versions)
            feature_versions_refs = []
            if feature_versions:
                for feature_version in feature_versions:
                    reliability_id = feature_version["_ReliabilityID"]
                    reliability_value = get_reference_value("Reliability", reliability_id)
                    version_id = feature_version["_ID"]
                    # print(f"feature_version :: {feature_version}")
                    date_period_array = feature_version["DatePeriod"]
                    date_period_value = []
                    if date_period_array:
                        # print(f"date_period_raw_array :: {date_period_array}")
                        for date_period in date_period_array:
                            date_period_value.append(parse_date_period(date_period))
                        # print(f"date_period_value :: {date_period_value}")
                    version_details = feature_version["VersionDetail"]
                    versions = []
                    if version_details:
                        for version_detail in version_details:
                            value = version_detail["_VALUE"]
                            detail_type_id = version_detail["_DetailTypeID"]
                            detail_type_value = "Unknown" if detail_type_id is None else get_reference_value(
                                "DetailType", detail_type_id)
                            detail_reference_id = version_detail["_DetailReferenceID"]
                            detail_reference_value = get_reference_value("DetailReference", detail_reference_id)
                            versions.append({
                                "value": value,
                                "detail_type_id": detail_type_id,
                                "detail_type": detail_type_value,
                                "detail_reference_id": detail_reference_id,
                                "detail_reference": detail_reference_value
                            })
                    version_locations = feature_version["VersionLocation"]
                    locations = []
                    if version_locations:
                        for version_location in version_locations:
                            location_id = version_location["_LocationID"]
                            locations.append(location_id)
                    feature_versions_refs.append({
                        "reliability_id": reliability_id,
                        "reliability_value": reliability_value,
                        "date_period": date_period_value,
                        "version_id": version_id,
                        "versions": versions,
                        "locations": locations
                    })
            feature = {
                "feature_id": feature_id,
                "feature_type_id": feature_type_id,
                "feature_type_value": feature_type_value,
                "feature_type_group_id": feature_type_group_id,
                "feature_type_group_value": feature_type_group_value,
                "feature_versions": feature_versions_refs
            }
            features.append(feature)
    return features