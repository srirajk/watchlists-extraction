import json

reference_values_json = "/Users/srirajkadimisetty/projects/spark-python-demo/output/reference_values_map.json"

# Load reference values from JSON to a dictionary
with open(reference_values_json, 'r') as file:
    reference_data = json.load(file)

def get_reference_value(map_type, key):
    return reference_data.get(map_type, {}).get(str(key), {}).get("_VALUE", "Unknown")

def get_reference_value_by_sub_key(map_type, key, sub_key):
    return reference_data.get(map_type, {}).get(str(key), {}).get(sub_key, "Unknown")

# Function to process a single Profile row
def enrich_profile_data_udf(profile_row):

    #print(profile_row)

    profile_id = profile_row["_ID"]
    party_sub_type_id = profile_row["_PartySubTypeID"]
    party_sub_type_value = get_reference_value("PartySubType", party_sub_type_id)
    party_type_id = get_reference_value_by_sub_key("PartySubType", party_sub_type_id, "_PartyTypeID")
    party_type_value = get_reference_value("PartyType", party_type_id)

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

                documented_names = []
                for documented_name in alias["DocumentedName"]:
                    documented_name_json = {}
                    for part in documented_name["DocumentedNamePart"]:
                        name_part_value = part["NamePartValue"]
                        group_id = name_part_value["_NamePartGroupID"]
                        mapped_value = name_part_values_with_ref.get(str(group_id), "Unknown")
                        documented_name_json[mapped_value] = name_part_value["_VALUE"]

                    documented_names.append(documented_name_json)

                # Append transformed alias details
                results.append({
                    "profile_id": profile_id,
                    "identity_id": identity_id,
                    "alias_type_value": alias_type_value,
                    "_Primary": alias["_Primary"],
                    "documented_names": documented_names,
                    "party_sub_type": party_sub_type_value,
                    "party_type": party_type_value
                })

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


def transform_document(row):
    """
    Transform a single row of id_reg_documents_df into a POJO-like record.
    """

    #print(row)
    identity_id = row["_IdentityID"]
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
        "document_dates": document_dates
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