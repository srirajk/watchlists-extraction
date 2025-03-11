import json

from pyspark.sql.functions import udf

from src.ofac.schemas import enriched_feature_schema

reference_values_json = "/Users/srirajkadimisetty/projects/watchlists-extraction/reference_data/ofac/reference_values_map.json"

# Load reference values from JSON to a dictionary
with open(reference_values_json, 'r') as file:
    reference_data = json.load(file)

def get_reference_value(map_type, key):
    return reference_data.get(map_type, {}).get(str(key), {}).get("_VALUE", "Unknown")

def get_reference_obj_by_key(map_type, key):
    return reference_data.get(map_type, {}).get(str(key), {})

def get_reference_value_by_sub_key(map_type, key, sub_key):
    return reference_data.get(map_type, {}).get(str(key), {}).get(sub_key, "Unknown")

# Function to process a single Profile row
def parse_date_period(date_period):

    calendar_type_id = date_period["_CalendarTypeID"]
    calendar_type_value = get_reference_value("CalendarType", calendar_type_id)

    start = date_period.Start if hasattr(date_period, "Start") and date_period.Start else None
    end = date_period.End if hasattr(date_period, "End") and date_period.End else None


    # Extract "From" and "To" values from Start
    start_from = start.From if start and hasattr(start, "From") else None
    start_to = start.To if start and hasattr(start, "To") else None

    # Extract "From" and "To" values from End
    end_from = end.From if end and hasattr(end, "From") else None
    end_to = end.To if end and hasattr(end, "To") else None

    # Determine if "Start" and "End" represent a single fixed date or a range
    start_date = {}
    if start_from and start_to and (
            start_from.Year == start_to.Year
            and start_from.Month == start_to.Month
            and start_from.Day == start_to.Day
    ):  # Single fixed date
        start_date["fixed"] = f"{start_from.Year}-{start_from.Month}-{start_from.Day}"
    else:  # Range
        start_date["range"] = {
            "from": f"{start_from.Year}-{start_from.Month}-{start_from.Day}" if start_from else "Unknown",
            "to": f"{start_to.Year}-{start_to.Month}-{start_to.Day}" if start_to else "Unknown"
        }

    end_date = {}
    if end_from and end_to and (
            end_from.Year == end_to.Year
            and end_from.Month == end_to.Month
            and end_from.Day == end_to.Day
    ):  # Single fixed date
        end_date["fixed"] = f"{end_from.Year}-{end_from.Month}-{end_from.Day}"
    else:  # Range
        end_date["range"] = {
            "from": f"{end_from.Year}-{end_from.Month}-{end_from.Day}" if end_from else "Unknown",
            "to": f"{end_to.Year}-{end_to.Month}-{end_to.Day}" if end_to else "Unknown"
        }

    return {
        "calendar_type_id": calendar_type_id,
        "calendar_type_value": calendar_type_value,
        "start_date": start_date,
        "end_date": end_date,
    }


def enrich_profile_data_udf(profile_row):

    #print(profile_row)

    profile_id = profile_row["_ID"]
    party_sub_type_id = profile_row["_PartySubTypeID"]
    party_sub_type_value = get_reference_value("PartySubType", party_sub_type_id)
    party_type_id = get_reference_value_by_sub_key("PartySubType", party_sub_type_id, "_PartyTypeID")
    party_type_value = get_reference_value("PartyType", party_type_id)

    features = []
    if "Feature" in profile_row and profile_row["Feature"]:
        for feature in profile_row["Feature"]:
            feature_id = feature["_ID"]
            feature_type_id = feature["_FeatureTypeID"]
            feature_type_value = get_reference_obj_by_key("FeatureType", feature_type_id)
            feature_type_group_id = feature_type_value.get("_FeatureTypeGroupID", None)
            feature_type_group_value = "Unknown" if feature_type_group_id is None else get_reference_value("FeatureTypeGroup", feature_type_group_id)

            feature_versions = feature["FeatureVersion"]
            #print(feature_versions)
            feature_versions_refs = []
            if feature_versions:
                for feature_version in feature_versions:
                    reliability_id = feature_version["_ReliabilityID"]
                    reliability_value = get_reference_value("Reliability", reliability_id)
                    version_id = feature_version["_ID"]
                    #print(f"feature_version :: {feature_version}")
                    date_period_array = feature_version["DatePeriod"]
                    date_period_value = []
                    if date_period_array:
                        #print(f"date_period_raw_array :: {date_period_array}")
                        for date_period in date_period_array:
                            date_period_value.append(parse_date_period(date_period))
                        #print(f"date_period_value :: {date_period_value}")
                    version_details = feature_version["VersionDetail"]
                    versions = []
                    if version_details:
                        for version_detail in version_details:
                            value = version_detail["_VALUE"]
                            detail_type_id = version_detail["_DetailTypeID"]
                            detail_type_value = "Unknown" if detail_type_id is None else get_reference_value("DetailType", detail_type_id)
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

            #print(feature)

    # Precompute NamePartGroup mappings
    name_part_values_with_ref = {}
    if "Identity" in profile_row and profile_row["Identity"]:
        for group in profile_row["Identity"][0]["NamePartGroups"]["MasterNamePartGroup"]:
            group_id = group["NamePartGroup"]["_ID"]
            name_part_type_id = group["NamePartGroup"]["_NamePartTypeID"]
            name_part_values_with_ref[str(group_id)] = get_reference_value("NamePartType", name_part_type_id)

    results = []
    if "Identity" in profile_row and profile_row["Identity"]:
        for identity in profile_row["Identity"]:  # Loop over all identities
            identity_id = identity["_ID"]
            for alias in identity["Alias"]:
                alias_type_id = alias["_AliasTypeID"]
                alias_type_value = get_reference_value("AliasType", alias_type_id)
                is_primary = alias["_Primary"]
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
                alias_dict = {
                    "profile_id": profile_id,
                    "identity_id": identity_id,
                    "alias_type_value": alias_type_value,
                    "_Primary": is_primary,
                    "documented_names": documented_names,
                    "party_sub_type": party_sub_type_value,
                    "party_type": party_type_value,
                }
                if is_primary:
                    alias_dict["feature"] = features
                else:
                    alias_dict["feature"] = {}
                # Append transformed alias details
                results.append(alias_dict)

    return results

def map_document_type(doc_type_id):
    """
    Map the _IDRegDocTypeID to a human-readable document type using the reference map.
    """
    return reference_data.get("IDRegDocType", {}).get(str(doc_type_id), {}).get("_VALUE", "Unknown")

def map_issuing_country(country_id):
    """
    Map the _IssuedBy-CountryID to a country description and value using the reference map.
    """
    country_info = reference_data.get("AreaCode", {}).get(str(country_id), {})
    description = country_info.get("_Description", "Unknown")
    value = country_info.get("_VALUE", "Unknown")
    return f"{description} ({value})"


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

                part = {
                    "location_party_type_value_id": location_party_type_value_id,
                    "location_party_type_value": location_party_type_value,
                    "location_part_value_status_id": location_part_value_status_id,
                    "location_part_value_status": location_part_value_status,
                    "location_value": location_value,
                    "is_primary": location_part_value["_Primary"],
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
        "id_document_refs":id_document_refs_values,
        "location_area": location_area_codes_values,
        "location_country": location_countries_values,
        "location_parts": location_parts_values
    }


def transform_document(row):
    """
    Transform a single row of id_reg_documents_df into a POJO-like record.
    """

    #print(row)
    identity_id = row["_IdentityID"]
    location_id = row["_IssuedIn-LocationID"]
    document_type = map_document_type(row["_IDRegDocTypeID"])
    issuing_country = map_issuing_country(row["_IssuedBy-CountryID"])
    id_reg_document_id = row["_ID"]
    id_registration_number = row["IDRegistrationNo"]._VALUE if row["IDRegistrationNo"] and "_VALUE" in row["IDRegistrationNo"] else "Unknown"
    document_dates = []



    # Extract document dates if available
    if row["DocumentDate"]:
        for date in row["DocumentDate"]:
            if date.DatePeriod:  # Ensure DatePeriod exists
                date_period = date.DatePeriod

                # Extract Start and End blocks
                start = date_period.Start if hasattr(date_period, "Start") and date_period.Start else None
                end = date_period.End if hasattr(date_period, "End") and date_period.End else None

                # Extract "From" and "To" values from Start
                start_from = start.From if start and hasattr(start, "From") else None
                start_to = start.To if start and hasattr(start, "To") else None

                # Extract "From" and "To" values from End
                end_from = end.From if end and hasattr(end, "From") else None
                end_to = end.To if end and hasattr(end, "To") else None

                # Determine if "Start" and "End" represent a single fixed date or a range
                start_date = {}
                if start_from and start_to and (
                        start_from.Year == start_to.Year
                        and start_from.Month == start_to.Month
                        and start_from.Day == start_to.Day
                ):  # Single fixed date
                    start_date["fixed"] = f"{start_from.Year}-{start_from.Month}-{start_from.Day}"
                else:  # Range
                    start_date["range"] = {
                        "from": f"{start_from.Year}-{start_from.Month}-{start_from.Day}" if start_from else "Unknown",
                        "to": f"{start_to.Year}-{start_to.Month}-{start_to.Day}" if start_to else "Unknown"
                    }

                end_date = {}
                if end_from and end_to and (
                        end_from.Year == end_to.Year
                        and end_from.Month == end_to.Month
                        and end_from.Day == end_to.Day
                ):  # Single fixed date
                    end_date["fixed"] = f"{end_from.Year}-{end_from.Month}-{end_from.Day}"
                else:  # Range
                    end_date["range"] = {
                        "from": f"{end_from.Year}-{end_from.Month}-{end_from.Day}" if end_from else "Unknown",
                        "to": f"{end_to.Year}-{end_to.Month}-{end_to.Day}" if end_to else "Unknown"
                    }

                document_dates.append({
                    "start_date": start_date,
                    "end_date": end_date
                })




    # Build the POJO-like record
    return {
        "identityId": identity_id,
        "document_type": document_type,
        "issuing_country": issuing_country,
        "id_reg_document_id": id_reg_document_id,
        "id_registration_number": id_registration_number,
        "document_dates": document_dates,
        "location_id": location_id
    }


# Define a UDF to extract name parts dynamically
def extract_names(documented_names):
    if not documented_names:  # Handle cases where the column is empty or None
        return None
    # Initialize empty name parts
    first_name = ""
    last_name = ""
    other_name_parts = []
    # Iterate through the list of name maps
    for name_map in documented_names:
        # check keys if first name and last name are present and if not then extract the value from whatever key and assign to other name
        if "First Name" in name_map:
            first_name = name_map["First Name"]
        elif "Last Name" in name_map:
            last_name = name_map["Last Name"]
        else:
            other_name_parts.append(next(iter(name_map.values()), ""))

    # Join other names into a single string
    other_name = " ".join(other_name_parts).strip()

    # Construct the result
    if first_name or last_name:
        return f"{last_name}, {first_name}" + (f" ({other_name})" if other_name else "")
    else:
        return other_name if other_name else None


@udf(enriched_feature_schema)
def enrich_features(original_feature, feature_enriched, is_primary):
    if not is_primary or not original_feature:
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

from typing import Any, Dict, List, Union
from pyspark.sql import Row

def deep_asdict(obj: Any) -> Union[Dict, List, Any]:
    """
    Recursively convert Row objects to dictionaries.
    
    Args:
        obj: The object to convert, which can be a Row, list, dict, or primitive type
        
    Returns:
        The converted object with all Row objects transformed to dictionaries
        
    Example:
        >>> row = Row(name="John", details=Row(age=30))
        >>> deep_asdict(row)
        {'name': 'John', 'details': {'age': 30}}
    """
    try:
        if isinstance(obj, list):
            return [deep_asdict(item) for item in obj]
        elif hasattr(obj, "asDict"):
            # Convert Row to dict
            d = obj.asDict()
            # Recursively convert any nested Row objects
            return {k: deep_asdict(v) for k, v in d.items()}
        elif isinstance(obj, dict):
            # Recursively convert any Row objects in dict values
            return {k: deep_asdict(v) for k, v in obj.items()}
        else:
            # Return primitive types as is
            return obj
    except RecursionError:
        # Handle potential recursion errors for deeply nested structures
        raise ValueError("Object structure is too deeply nested for conversion")
    except Exception as e:
        # Catch any other unexpected errors
        raise ValueError(f"Error converting object: {str(e)}")
