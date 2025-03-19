from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, MapType

# Define the schema for Start and End fields within DatePeriod
boundary_struct_schema = StructType([
    StructField("From", StructType([
        StructField("Year", LongType(), True),
        StructField("Month", LongType(), True),
        StructField("Day", LongType(), True)
    ]), True),
    StructField("To", StructType([
        StructField("Year", LongType(), True),
        StructField("Month", LongType(), True),
        StructField("Day", LongType(), True)
    ]), True),
    StructField("_Approximate", BooleanType(), True),
    StructField("_YearFixed", BooleanType(), True),
    StructField("_MonthFixed", BooleanType(), True),
    StructField("_DayFixed", BooleanType(), True)
])

duration_schema = StructType([
    StructField("Years", LongType(), True),
    StructField("Months", LongType(), True),
    StructField("Days", LongType(), True),
    StructField("_Approximate", BooleanType(), True)
])

# Define the schema for DatePeriod
date_period_schema = StructType([
    StructField("Start", boundary_struct_schema, True),  # Start boundary
    StructField("End", boundary_struct_schema, True),  # End boundary
    StructField("_CalendarTypeID", LongType(), True),
    StructField("_YearFixed", BooleanType(), True),
    StructField("_MonthFixed", BooleanType(), True),
    StructField("_DayFixed", BooleanType(), True),
    StructField("DurationMinimum", duration_schema, True),
    StructField("DurationMaximum", duration_schema, True),
])

return_date_boundary_schema = StructType([
        StructField("fixed", StringType(), True),
        StructField("range", StructType([
            StructField("from", StringType(), True),
            StructField("to", StringType(), True)
        ]), True),
    ]
)

return_date_period_schema = StructType(
    [
        StructField("start_date", return_date_boundary_schema, True),
        StructField("end_date", return_date_boundary_schema, True)
    ]
)


distinct_party_schema = StructType([
    StructField("_FixedRef", StringType(), True),
    StructField("_DeltaAction", StringType(), True),
    StructField("Comment", ArrayType(StringType()), True),
    StructField("Profile", ArrayType(StructType([
        StructField("_ID", LongType(), True),
        StructField("_PartySubTypeID", LongType(), True),
        StructField("_DeltaAction", StringType(), True),
        StructField("Comment", ArrayType(StringType()), True),
        StructField("Identity", ArrayType(StructType([
            StructField("_ID", LongType(), True),
            StructField("_FixedRef", LongType(), True),
            StructField("_Primary", BooleanType(), True),
            StructField("_False", BooleanType(), True),
            StructField("Alias", ArrayType(StructType([
                StructField("_FixedRef", LongType(), True),
                StructField("_AliasTypeID", LongType(), True),
                StructField("_Primary", BooleanType(), True),
                StructField("_LowQuality", BooleanType(), True),
                StructField("DocumentedName", ArrayType(StructType([
                    StructField("_ID", LongType(), True),
                    StructField("_FixedRef", LongType(), True),
                    StructField("_DocNameStatusID", LongType(), True),
                    StructField("DocumentedNamePart", ArrayType(StructType([
                        StructField("NamePartValue", StructType([
                            StructField("_NamePartGroupID", LongType(), True),
                            StructField("_ScriptID", LongType(), True),
                            StructField("_ScriptStatusID", LongType(), True),
                            StructField("_Acronym", BooleanType(), True),
                            StructField("_VALUE", StringType(), True)
                        ]))
                    ])))
                ])))
            ]))),
            StructField("NamePartGroups", StructType([
                StructField("MasterNamePartGroup", ArrayType(StructType([
                    StructField("NamePartGroup", StructType([
                        StructField("_ID", LongType(), True),
                        StructField("_NamePartTypeID", LongType(), True)
                    ]))
                ])))
            ]))
        ]))),
        StructField("Feature", ArrayType(StructType([
            StructField("_ID", LongType(), True),
            StructField("_FeatureTypeID", LongType(), True),
            StructField("FeatureVersion", ArrayType(StructType([
                StructField("_ReliabilityID", LongType(), True),
                StructField("_ID", LongType(), True),
                StructField("Comment", StringType(), True),
                StructField("DatePeriod", ArrayType(date_period_schema), True),
                StructField("VersionDetail", ArrayType(StructType([
                  StructField("_DetailReferenceID", StringType(), True),
                  StructField("_DetailTypeID", LongType(), True),
                  StructField("_VALUE", StringType(), True)
                ])), True),
                StructField("VersionLocation", ArrayType(StructType([
                    StructField("_LocationID", LongType(), True)
                ])), True)
            ])), True),
            StructField("IdentityReference", ArrayType(StructType([
                StructField("_IdentityID", LongType(), True),
                StructField("_IdentityFeatureLinkTypeID", LongType(), True)
            ])), True)
        ]))),
        StructField("SanctionsEntryReference", ArrayType(StructType([
            StructField("_SanctionsEntryID", LongType(), True),
            StructField("_DeltaAction", StringType(), True)
        ]))),
        StructField("ExternalReference", ArrayType(StructType([
            StructField("Comment", StringType(), True),
            StructField("ExRefValue", StructType([
                StructField("_DeltaAction", StringType(), True),
                StructField("_VALUE", StringType(), True)
            ])),
            StructField("DirectURL", StringType(), True),
            StructField("SubLink", ArrayType(StructType([
                StructField("Description", StructType([
                    StructField("_DeltaAction", StringType(), True),
                    StructField("_VALUE", StringType(), True)
                ])),
                StructField("DirectURL", StringType(), True),
                StructField("_TargetTypeID", LongType(), True),
                StructField("_DeltaAction", StringType(), True)
            ]))),
            StructField("_ExRefTypeID", LongType(), True),
            StructField("_DeltaAction", StringType(), True)
        ])))
    ])))
])



# Define the schema for DocumentDate
document_date_schema = StructType([
    StructField("DatePeriod", StructType([
        StructField("Start", boundary_struct_schema, True),
        StructField("End", boundary_struct_schema, True),
        StructField("_CalendarTypeID", LongType(), True),
        StructField("_YearFixed", BooleanType(), True),
        StructField("_MonthFixed", BooleanType(), True),
        StructField("_DayFixed", BooleanType(), True)
    ]), True),
    StructField("_IDRegDocDateTypeID", LongType(), True),
    StructField("_DeltaAction", StringType(), True)
])



# Integrate into the IDRegDocument schema
id_reg_documents_schema = StructType([
    StructField("_ID", LongType(), True),
    StructField("_IDRegDocTypeID", LongType(), True),
    StructField("_IdentityID", LongType(), True),
    StructField("_IssuedBy-CountryID", LongType(), True),
    StructField("_IssuedIn-LocationID", LongType(), True),
    StructField("_ValidityID", LongType(), True),
    StructField("_DeltaAction", StringType(), True),
    StructField("Comment", StringType(), True),
    StructField("IDRegistrationNo", StructType([
        StructField("_VALUE", StringType(), True)
    ]), True),
    StructField("IssuingAuthority", StructType([
        StructField("_VALUE", StringType(), True)
    ]), True),
    StructField("DocumentDate", ArrayType(document_date_schema), True),
    StructField("DocumentedNameReference", ArrayType(StructType([
        StructField("_DocumentedNameID", LongType(), True)
    ])), True)
])



# Location Schema

location_schema = StructType([
    StructField("_ID", LongType(), True),
    StructField("FeatureVersionReference", ArrayType(StructType([
        StructField("_FeatureVersionID", LongType(), True),
        StructField("_VALUE", StringType(), True)
    ])), True),
    StructField("IDRegDocumentReference", ArrayType(StructType([
        StructField("_IDRegDocumentID", LongType(), True),
        StructField("_VALUE", StringType(), True)
    ])), True),
    StructField("LocationAreaCode", ArrayType(StructType([
        StructField("_AreaCodeID", LongType(), True),
        StructField("_VALUE", StringType(), True)
    ])), True),
    StructField("LocationCountry", ArrayType(StructType([
        StructField("_CountryID", LongType(), True),
        StructField("_CountryRelevanceID", LongType(), True),
        StructField("_VALUE", StringType(), True)
    ])), True),
    StructField("LocationPart", ArrayType(StructType([
        StructField("_LocPartTypeID", LongType(), True),
        StructField("LocationPartValue", ArrayType(StructType([
            StructField("Comment", StringType(), True),
            StructField("Value", StringType(), True),
            StructField("_LocPartValueStatusID", LongType(), True),
            StructField("_LocPartValueTypeID", LongType(), True),
            StructField("_Primary", BooleanType(), True)
        ])), True)
    ])), True)
])

date_schema = StructType([
    StructField("Day", LongType(), True),
    StructField("Month", LongType(), True),
    StructField("Year", LongType(), True),
    StructField("_CalendarTypeID", LongType(), True)
])

# Define the schema for EntryEvent
entry_event_schema = StructType([
    StructField("Comment", ArrayType(StringType()), True),
    StructField("Date", date_schema, True),
    StructField("_EntryEventTypeID", LongType(), True),
    StructField("_ID", LongType(), True),
    StructField("_LegalBasisID", LongType(), True)
])

# Define the schema for SanctionsMeasure
sanctions_measure_schema = StructType([
    StructField("Comment", ArrayType(StringType()), True),
    StructField("DatePeriod", date_period_schema, True),
    StructField("_ID", LongType(), True),
    StructField("_SanctionsTypeID", LongType(), True)
])

sanctions_entry_schema = StructType([
    StructField("EntryEvent", ArrayType(entry_event_schema), True),
    StructField("SanctionsMeasure", ArrayType(sanctions_measure_schema), True),
    StructField("_ID", LongType(), True),
    StructField("_ListID", LongType(), True),
    StructField("_ProfileID", LongType(), True)
])


# Schema for version details in feature_versions
version_detail_schema = StructType([
    StructField("value", StringType(), True),
    StructField("detail_type_id", LongType(), True),
    StructField("detail_type", StringType(), True),
    StructField("detail_reference_id", LongType(), True),
    StructField("detail_reference", StringType(), True)
])

# Schema for feature versions
feature_version_schema = StructType([
    StructField("reliability_id", LongType(), True),
    StructField("reliability_value", StringType(), True),
    StructField("version_id", LongType(), True),
    StructField("date_period", ArrayType(return_date_period_schema), True),
    StructField("versions", ArrayType(version_detail_schema), True),
    StructField("locations", ArrayType(LongType()), True)
])

# Schema for feature
feature_schema = ArrayType(StructType([
    StructField("feature_id", LongType(), True),
    StructField("feature_type_id", LongType(), True),
    StructField("feature_type_value", MapType(StringType(), StringType()), True),
    StructField("feature_type_group_id", LongType(), True),
    StructField("feature_type_group_value", StringType(), True),
    StructField("feature_versions", ArrayType(feature_version_schema), True)
]), True)

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


"""
enriched_sanction_measures_schema = StructType([
    StructType([
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
])
"""

# Fix the return_date_period_schema definition
return_date_period_schema = StructType([
    StructField("start_date", return_date_boundary_schema, True),
    StructField("end_date", return_date_boundary_schema, True),
    StructField("calendar_type_id", StringType(), True),
    StructField("calendar_type_value", StringType(), True)
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

enrich_sanction_entries_schema = StructType([
    StructField("profile_id", StringType(), True),
    StructField("list_id", StringType(), True),
    StructField("list_value", StringType(), True),
    StructField("sanction_entry_id", StringType(), True),
    StructField("entry_events", ArrayType(enriched_sanction_entry_events_schema), True),
    StructField("sanctions_measures", ArrayType(enriched_sanction_measures_schema), True),
    #StructField("sanctions_measures", ArrayType(MapType(StringType(), StringType())), True)
])

# Define the schema for Relation
profile_relation_schema = StructType([
    StructField("_ID", LongType(), True),
    StructField("_From-ProfileID", LongType(), True),
    StructField("_To-ProfileID", LongType(), True),
    StructField("_RelationTypeID", LongType(), True),
    StructField("_RelationQualityID", LongType(), True),
    StructField("_SanctionsEntryID", LongType(), True),
    StructField("_Former", BooleanType(), True),
    StructField("Comment", StringType(), True),
    StructField("DatePeriod", date_period_schema, True),
    StructField("IDRegDocumentReference", ArrayType(StructType([
        StructField("_IDRegDocumentID", LongType(), True),
        StructField("_VALUE", StringType(), True)
    ])), True),
])

return_profile_relation_schema = StructType([
    StructField("relationship_id", LongType(), True),
    StructField("to_profile_id", LongType(), True),
    StructField("relation_type_id", LongType(), True),
    StructField("relation_quality_id", LongType(), True),
    StructField("sanctions_entry_id", LongType(), True),
    StructField("former", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("relation_quality", StringType(), True),
    StructField("relation_type", StringType(), True),
    StructField("date_period", return_date_period_schema, True),
    StructField("to_profile_documented_name", ArrayType(MapType(StringType(), StringType())), True),
])