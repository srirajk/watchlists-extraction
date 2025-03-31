from pyspark.sql.functions import udf, struct, col, collect_list, to_json, md5

from src.ofac.custom_udfs import get_reference_value, parse_date_period, parse_date, get_reference_obj_by_key
from src.ofac.medallion.schemas.sanction_entry_schema import sanction_entries_schema


def get_sanction_entries_by_profile_id(spark, sanction_entries_df):
    sanctions_entries_df = sanction_entries_df.withColumn("enriched_sanction_entries",
                                                                   enrich_sanction_entries(
                                                                       struct(*sanction_entries_df.columns)))

    sanctions_entries_df = sanctions_entries_df.select(
        col("_ProfileID").alias("profile_id"),
        col("enriched_sanction_entries").alias("sanction_entry"),
    )
    enriched_sanctions_entries_df = sanctions_entries_df.groupBy("profile_id").agg(
        collect_list("sanction_entry").alias("sanction_entries"))

    enriched_sanctions_entries_df = enriched_sanctions_entries_df.withColumn("sanctions_entries_hash", md5(to_json(col("sanction_entries"))))

    return enriched_sanctions_entries_df



@udf(sanction_entries_schema)
#@udf(MapType(StringType(), StringType()))
def enrich_sanction_entries(sanction_entry_row):
    #print(f"sanction_entry_row :: {sanction_entry_row}")
    profile_id = sanction_entry_row["_ProfileID"]
    list_id = sanction_entry_row["_ListID"]
    list_value = get_reference_value("List", list_id)
    sanction_entry_id = sanction_entry_row["_ID"]
    entry_events = sanction_entry_row["EntryEvent"]
    #print(f"entry_events :: {entry_events}")
    entry_event_values = []
    if entry_events:
        for entry_event in entry_events:
            entry_event_id = entry_event["_ID"]
            comments = entry_event["Comment"]
            date = entry_event["Date"]
            date_val = None
            if date:
                date_val = parse_date(date)
            entry_event_type_id = entry_event["_EntryEventTypeID"]
            entry_event_type_value = get_reference_value("EntryEventType", entry_event_type_id)
            legal_basis_id = entry_event["_LegalBasisID"]
            legal_basis_details = get_legal_details(legal_basis_id)
            entry_event_values.append({
                "entry_event_id": entry_event_id,
                "comments": comments,
                "date": date_val,
                "entry_event_type_id": entry_event_type_id,
                "entry_event_type_value": entry_event_type_value,
                "legal_basis_details": legal_basis_details
            })
    #print(f"entry_event_values :: {entry_event_values}")
    sanctions_measures = sanction_entry_row["SanctionsMeasure"]
    sanctions_measure_values = []
    if sanctions_measures:
        for sanctions_measure in sanctions_measures:
            sanctions_measure_id = sanctions_measure["_ID"]
            sanctions_type_id = sanctions_measure["_SanctionsTypeID"]
            sanctions_type_value = get_reference_value("SanctionsType", sanctions_type_id)
            date_period = sanctions_measure["DatePeriod"]
            date_period_value = None
            if date_period:
                date_period_value = parse_date_period(date_period)
            comments = sanctions_measure["Comment"]

            sanctions_measure_values.append({
                "sanctions_measure_id": sanctions_measure_id,
                "sanctions_type_id": sanctions_type_id,
                "sanctions_type_value": sanctions_type_value,
                "comments": comments,
                "date_period": date_period_value,
            })

    res =   {
        #"profile_id": profile_id,
        "list_id": list_id,
        "list_value": list_value,
        "sanction_entry_id": sanction_entry_id,
        "entry_events": entry_event_values,
        "sanctions_measures": sanctions_measure_values
    }

    # if profile_id == 36:
    #     print(f"res :: {res}")

    return res


def get_legal_details(legal_basis_id):
    legal_basis_obj = get_reference_obj_by_key("LegalBasis", legal_basis_id)
    #print(legal_basis_obj)
    # if legal_basis_obj:
    #     return {
    #         "legal_basis_id": legal_basis_id,
    #     }

    legal_basis_type_id = legal_basis_obj.get("_LegalBasisTypeID")
    legal_basis_type_value = get_reference_value("LegalBasisType", legal_basis_type_id)
    legal_basis_value = legal_basis_obj.get("_VALUE")
    legal_basis_short_ref = legal_basis_obj.get("_LegalBasisShortRef")

    # sanctions_program
    sanctions_program_id = legal_basis_obj.get("_SanctionsProgramID")
    sanctions_program_object = get_reference_obj_by_key("SanctionsProgram", sanctions_program_id)
    sanctions_program_value = sanctions_program_object.get("_VALUE")
    subsidary_body_id = sanctions_program_object.get("_SubsidiaryBodyID")
    subsidiary_body_object = get_reference_obj_by_key("SubsidiaryBody", subsidary_body_id)
    subsidiary_body_value = subsidiary_body_object.get("_VALUE")
    decision_making_body_id = subsidiary_body_object.get("_DecisionMakingBodyID")
    decision_making_body_object = get_reference_obj_by_key("DecisionMakingBody", decision_making_body_id)
    sanctions_program = {
        "sanctions_program_id": sanctions_program_id,
        "sanctions_program_value": sanctions_program_value,
        "subsidiary_body_id": subsidary_body_id,
        "subsidiary_body_value": subsidiary_body_value,
        "decision_making_body_id": decision_making_body_id,
        "decision_making_body_organization_id": decision_making_body_object.get("_OrganisationID"),
        "decision_making_body_value": decision_making_body_object.get("_VALUE"),
    }
    return {
        "legal_basis_id": legal_basis_id,
        "legal_basis_value": legal_basis_value,
        "legal_basis_type_id": legal_basis_type_id,
        "legal_basis_type_value": legal_basis_type_value,
        "legal_basis_short_ref": legal_basis_short_ref,
        "sanctions_program": sanctions_program
    }