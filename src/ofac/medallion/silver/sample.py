from collections import namedtuple
from hashlib import md5
import json

# Define a namedtuple to simulate Row
Row = namedtuple('Row', ['action', 'data'])

def compare_alias_data(new_data, existing_data):
    """
    Compares new alias data with existing data to determine whether records should be inserted, updated, or deleted.
    Returns a list of decisions.
    """
    decisions = []

    print(f"New Data: {new_data}")
    print(f"Existing Data: {existing_data}")

    if existing_data is None:
        for record in new_data:
            decisions.append(Row(action="insert", data=record))
    else:
        existing_aliases = {record["alias_hash"]: record for record in existing_data}

        for new_record in new_data:
            alias_hash = new_record["alias_hash"]

            if alias_hash in existing_aliases:
                existing_record = existing_aliases[alias_hash]

                if new_record["is_primary"]:
                    if new_record["documents_hash"] != existing_record["documents_hash"]:
                        # Deactivate the existing record
                        existing_record_inactive = dict(existing_record)
                        existing_record_inactive["active_flag"] = "N"
                        decisions.append(Row(action="update", data=existing_record_inactive))

                        # Create a new active record with incremented version
                        updated_record = dict(new_record)
                        updated_record["alias_id"] = existing_record["alias_id"]
                        updated_record["app_profile_id"] = existing_record["app_profile_id"]
                        updated_record["version"] = existing_record["version"] + 1
                        updated_record["active_flag"] = "Y"
                        decisions.append(Row(action="insert", data=updated_record))
                    else:
                        decisions.append(Row(action="no_change", data=dict(existing_record)))
                else:
                    decisions.append(Row(action="no_change", data=dict(existing_record)))
            else:
                new_record_dict = dict(new_record)
                new_record_dict["version"] = 1
                new_record_dict["active_flag"] = "Y"
                new_record_dict["updated_at"] = None
                new_record_dict["end_date"] = None
                decisions.append(Row(action="insert", data=new_record_dict))

        # Handle deletions
        existing_alias_hashes = {r["alias_hash"] for r in new_data}
        for existing_record in existing_data:
            if existing_record["alias_hash"] not in existing_alias_hashes:
                deleted_record = dict(existing_record)
                deleted_record["active_flag"] = "N"
                decisions.append(Row(action="delete", data=deleted_record))

    return decisions

# Test the function with your sample data
new_data = [
    {
        "identity_id": 4375,
        "alias_type_value": "Name",
        "is_primary": True,
        "documented_names": [{"Entity Name": "AEROCARIBBEAN AIRLINES 4"}],
        "extraction_timestamp": "2025-02-02 17:57:00",
        "source_name": "OFAC",
        "_FixedRef": "36",
        "party_sub_type": "Unknown",
        "party_type": "Entity",
        "documented_names_hash": "a5fe4a133c735c0492461ede06fffe70",
        "alias_id": "4d4cfcf465449d84fbc386f95bdfeace",
        "app_profile_id": "APP_19ca14e7ea6328a42e0eb13d585e4c22",
        "alias_hash": "5c13a0bb2f0e5b49c97348a0ae478943f0",
        "id_documents": [
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "2", "id_registration_number": "040070827", "document_dates": "[]"},
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "3", "id_registration_number": "07040059504", "document_dates": "[]"},
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "3", "id_registration_number": "070476559504", "document_dates": "[]"}
        ],
        "documents_hash": "09e63c9c963d3a55b709793b40e7e630"
    }
]

existing_data = [
    {
        "identity_id": 4375,
        "alias_type_value": "A.K.A.",
        "is_primary": False,
        "documented_names": [{"Entity Name": "AERO-CARIBBEAN"}],
        "extraction_timestamp": "2025-02-02 14:34:00",
        "source_name": "OFAC",
        "_FixedRef": "36",
        "party_sub_type": "Unknown",
        "party_type": "Entity",
        "app_profile_id": "APP_19ca14e7ea6328a42e0eb13d585e4c22",
        "alias_id": "116a005f0a2f631f46825c3418e5f2e2",
        "alias_hash": "68c0cec691cc75a1369f4a1b812f25b0",
        "id_documents": None,
        "documents_hash": None,
        "version": 1,
        "active_flag": "Y",
        "updated_at": "2025-02-02 17:37:00",
        "end_date": None
    },
    {
        "identity_id": 4375,
        "alias_type_value": "Name",
        "is_primary": True,
        "documented_names": [{"Entity Name": "AEROCARIBBEAN AIRLINES"}],
        "extraction_timestamp": "2025-02-02 14:34:00",
        "source_name": "OFAC",
        "_FixedRef": "36",
        "party_sub_type": "Unknown",
        "party_type": "Entity",
        "app_profile_id": "APP_19ca14e7ea6328a42e0eb13d585e4c22",
        "alias_id": "4d4cfcf465449d84fbc386f95bdfeace",
        "alias_hash": "5c13a0bb2f0e5b49c97348a0ae4443f0",
        "id_documents": [
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "2", "id_registration_number": "040070827", "document_dates": "[]"},
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "3", "id_registration_number": "07040059504", "document_dates": "[]"}
        ],
        "documents_hash": "4be89a1acc0e6ba33c5ebefb88a0d1af",
        "version": 1,
        "active_flag": "N",
        "updated_at": "2025-02-02 17:37:00",
        "end_date": None
    },
    {
        "identity_id": 4375,
        "alias_type_value": "Name",
        "is_primary": True,
        "documented_names": [{"Entity Name": "AEROCARIBBEAN AIRLINES NEW"}],
        "extraction_timestamp": "2025-02-02 14:34:00",
        "source_name": "OFAC",
        "_FixedRef": "36",
        "party_sub_type": "Unknown",
        "party_type": "Entity",
        "app_profile_id": "APP_19ca14e7ea6328a42e0eb13d585e4c22",
        "alias_id": "4d4cfcf465449d84fbc386f95bdfe78e",
        "alias_hash": "5c13a0bb2f0e5b49c97348a0ae4443f0",
        "id_documents": [
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "2", "id_registration_number": "040070827", "document_dates": "[]"},
            {"document_type": "Passport", "issuing_country": "Mexico (MX)", "id_reg_document_id": "3", "id_registration_number": "07040059504", "document_dates": "[]"}
        ],
        "documents_hash": "4be89a1acc0e6ba33c5ebefb88a0d1af",
        "version": 2,
        "active_flag": "Y",
        "updated_at": "2025-02-02 17:37:00",
        "end_date": None
    }
]

# Run the test
decisions = compare_alias_data(new_data, existing_data)

# Print the results
print("\nDecisions:")
for decision in decisions:
    print(f"Action: {decision.action}")
    print(f"Data: {json.dumps(decision.data, indent=2)}")
    print()