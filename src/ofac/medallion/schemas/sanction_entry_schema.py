from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)

from src.ofac.medallion.schemas.common_schema import return_date_boundary_schema

enriched_sanction_entry_legals_basis_schema = StructType([
    StructField("legal_basis_id", StringType(), True),
    StructField("legal_basis_value", StringType(), True),
    StructField("legal_basis_type_id", StringType(), True),
    StructField("legal_basis_type_value", StringType(), True),
    StructField("legal_basis_short_ref", StringType(), True),
    StructField("sanctions_program", StructType([
        StructField("sanctions_program_id", LongType(), True),
        StructField("sanctions_program_value", StringType(), True),
        StructField("subsidiary_body_id", StringType(), True),
        StructField("subsidiary_body_value", StringType(), True),
        StructField("decision_making_body_id", LongType(), True),
        StructField("decision_making_body_organization_id", StringType(), True),
        StructField("decision_making_body_value", StringType(), True)
    ]), True)
])

enriched_sanction_entry_events_schema = StructType([
    StructField("entry_event_id", StringType(), True),
    StructField("comments", ArrayType(StringType()), True),
    StructField("entry_event_type_id", StringType(), True),
    StructField("entry_event_type_value", StringType(), True),
    StructField("legal_basis_details", enriched_sanction_entry_legals_basis_schema, True),
    StructField("date", StructType([
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("calendar_type_id", StringType(), True),
        StructField("calendar_type_value", StringType(), True),
    ]), True)
])

enriched_sanction_measures_schema = StructType([
    StructField("sanctions_measure_id", LongType(), True),
    StructField("sanctions_type_id", LongType(), True),
    StructField("sanctions_type_value", StringType(), True),
    StructField("comments", ArrayType(StringType()), True),
    StructField("date_period", StructType([
        StructField("start_date", return_date_boundary_schema, True),
        StructField("end_date", return_date_boundary_schema, True),
        StructField("calendar_type_id", StringType(), True),
        StructField("calendar_type_value", StringType(), True),
    ]), True)
])

sanction_entries_schema = StructType([
    StructField("profile_id", StringType(), True),
    StructField("list_id", StringType(), True),
    StructField("list_value", StringType(), True),
    StructField("sanction_entry_id", StringType(), True),
    StructField("entry_events", ArrayType(enriched_sanction_entry_events_schema), True),
    StructField("sanctions_measures", ArrayType(enriched_sanction_measures_schema), True),
])
