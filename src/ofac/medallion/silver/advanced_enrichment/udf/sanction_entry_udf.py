from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructField, StringType, StructType

from src.ofac.medallion.silver.advanced_enrichment.udf.common_udf import CommonUDFs


class SanctionEntryUDfs:
    enriched_sanction_entries_schema = ArrayType(
        StructType([
            StructField("list_value", StringType(), True),
            StructField("entry_events", ArrayType(
                StructType([
                    StructField("comments", ArrayType(StringType()), True),
                    StructField("entry_event_type", StringType(), True),
                    StructField("event_date", StructType([
                        StructField("date", StringType(), True),
                        StructField("calendar_type", StringType(), True)
                    ]), True),
                    StructField("legal_basis_details", StructType([
                        StructField("legal_basis_value", StringType(), True),
                        StructField("legal_basis_short_ref", StringType(), True),
                        StructField("sanctions_program", StructType([
                            StructField("program_value", StringType(), True),
                            StructField("subsidiary_body_value", StringType(), True),
                            StructField("decision_making_body_value", StringType(), True)
                        ]), True)
                    ]), True),
                ])
            ), True),
            StructField("sanctions_measures", ArrayType(
                StructType([
                    StructField("sanctions_type_value", ArrayType(StringType()), True),
                    StructField("comments", StringType(), True),
                    StructField("date_period", CommonUDFs.enriched_date_period_schema, True)
                ])
            ), True)
        ])
    )

    @ staticmethod
    @udf(returnType=enriched_sanction_entries_schema)
    def enrich_sanction_entries(sanction_entries):

        if sanction_entries is None:
            return None

        se_arr = []

        for sanction_entry in sanction_entries:
            list_value = sanction_entry.list_value
            se = {
                "list_value": list_value
            }
            if sanction_entry.entry_events is not None and len(sanction_entry.entry_events) > 0:
                se["entry_events"] = SanctionEntryUDfs.__enrich_entry_events__(sanction_entry.entry_events)
            if sanction_entry.sanctions_measures is not None and len(sanction_entry.sanctions_measures) > 0:
                sm_arr = []
                for sanction_measure in sanction_entry.sanctions_measures:
                    sm = {"sanctions_type_value": sanction_measure.sanctions_type_value}
                    if sanction_measure.comments is not None and len(sanction_measure.comments) > 0:
                        sm["comments"] = sanction_measure.comments
                    if sanction_measure.date_period is not None:
                        sm["date_period"] = CommonUDFs.enrich_date_period(sanction_measure.date_period)
                    sm_arr.append(sm)
                se["sanctions_measures"] = sm_arr
            se_arr.append(se)
        return se_arr

    @staticmethod
    def __enrich_entry_events__(entry_events):
        ee_arr = []
        for entry_event in entry_events:
            ee = {}
            if entry_event.comments is not None and len(entry_event.comments) > 0:
                ee["comments"] = entry_event.comments
            ee["entry_event_type"] = entry_event.entry_event_type_value

            if entry_event.date is not None:
                date = entry_event.date
                d = date.year + "-" + date.month + "-" + date.day
                ee["event_date"] = {
                    "date": d,
                    "calendar_type": date.calendar_type_value
                }

            if entry_event.legal_basis_details is not None:
                ld = {}
                legal_details = entry_event.legal_basis_details
                legal_basis_value = legal_details.legal_basis_value
                legal_basis_short_ref = legal_details.legal_basis_short_ref
                ld["legal_basis_value"] = legal_basis_value
                ld["legal_basis_short_ref"] = legal_basis_short_ref
                program = legal_details.sanctions_program
                p = {"program_value": program.sanctions_program_value,
                     "subsidiary_body_value": program.subsidiary_body_value,
                     "decision_making_body_value": program.decision_making_body_value}
                ld["sanctions_program"] = p
                ee["legal_basis_details"] = ld
            ee_arr.append(ee)
        return ee_arr
