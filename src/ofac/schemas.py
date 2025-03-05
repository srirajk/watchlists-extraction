from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, MapType

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

# Define the schema for DatePeriod
date_period_schema = StructType([
    StructField("Start", boundary_struct_schema, True),  # Start boundary
    StructField("End", boundary_struct_schema, True),  # End boundary
    StructField("_CalendarTypeID", LongType(), True),
    StructField("_YearFixed", BooleanType(), True),
    StructField("_MonthFixed", BooleanType(), True),
    StructField("_DayFixed", BooleanType(), True)
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



# # Define the schema for DateBoundarySchemaType
# date_boundary_schema = StructType([
#     StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#     StructField("_BoundaryQualifierTypeID", LongType(), True),  # Attribute: BoundaryQualifierTypeID
#     StructField("_VALUE", StringType(), True)  # Element value (actual date string)
# ])
#
# # Define the schema for DurationSchemaType
# duration_schema = StructType([
#     StructField("Years", StructType([
#         StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#         StructField("_VALUE", LongType(), True)  # Element value: Years
#     ]), True),
#     StructField("Months", StructType([
#         StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#         StructField("_VALUE", LongType(), True)  # Element value: Months
#     ]), True),
#     StructField("Days", StructType([
#         StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#         StructField("_VALUE", LongType(), True)  # Element value: Days
#     ]), True),
#     StructField("_Approximate", BooleanType(), True),  # Attribute: Approximate
#     StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
# ])
#
# # Define the schema for DatePeriod
# date_period_schema = StructType([
#     StructField("Comment", StringType(), True),  # Optional Comment
#     StructField("Start", StructType([
#         StructField("From", StructType([
#             StructField("Year", LongType(), True),
#             StructField("Month", LongType(), True),
#             StructField("Day", LongType(), True)
#         ]), True),
#         StructField("To", StructType([
#             StructField("Year", LongType(), True),
#             StructField("Month", LongType(), True),
#             StructField("Day", LongType(), True)
#         ]), True),
#         StructField("_Approximate", BooleanType(), True),  # Approximate attribute in Start
#         StructField("_DeltaAction", StringType(), True)  # DeltaAction in Start
#     ]), True),
#     StructField("End", StructType([
#         StructField("From", StructType([
#             StructField("Year", LongType(), True),
#             StructField("Month", LongType(), True),
#             StructField("Day", LongType(), True)
#         ]), True),
#         StructField("To", StructType([
#             StructField("Year", LongType(), True),
#             StructField("Month", LongType(), True),
#             StructField("Day", LongType(), True)
#         ]), True),
#         StructField("_Approximate", BooleanType(), True),  # Approximate attribute in End
#         StructField("_DeltaAction", StringType(), True)  # DeltaAction in End
#     ]), True),
#     StructField("DurationMinimum", duration_schema, True),  # Nested DurationMinimum
#     StructField("DurationMaximum", duration_schema, True),  # Nested DurationMaximum
#     StructField("_CalendarTypeID", LongType(), True),  # Attribute: CalendarTypeID
#     StructField("_YearFixed", BooleanType(), True),  # Attribute: YearFixed
#     StructField("_MonthFixed", BooleanType(), True),  # Attribute: MonthFixed
#     StructField("_DayFixed", BooleanType(), True),  # Attribute: DayFixed
#     StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
# ])
#
# # Define the schema for DocumentDate, including DatePeriod
# document_date_schema = StructType([
#     StructField("DatePeriod", ArrayType(date_period_schema), True),  # Nested DatePeriod structure
#     StructField("_IDRegDocDateTypeID", LongType(), True),  # Attribute: IDRegDocDateTypeID
#     StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
# ])
#
# # Define the complete schema for <IDRegDocuments>
# id_reg_documents_schema = StructType([
#     StructField("_ID", LongType(), True),  # Attribute: ID
#     StructField("_IDRegDocTypeID", LongType(), True),  # Attribute: IDRegDocTypeID
#     StructField("_IdentityID", LongType(), True),  # Attribute: IdentityID
#     StructField("_IssuedBy-CountryID", LongType(), True),  # Attribute: IssuedBy-CountryID
#     StructField("_IssuedIn-LocationID", LongType(), True),  # Attribute: IssuedIn-LocationID
#     StructField("_ValidityID", LongType(), True),  # Attribute: ValidityID
#     StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#
#     # Comment element
#     StructField("Comment", StringType(), True),
#
#     # IDRegistrationNo element
#     StructField("IDRegistrationNo", StructType([
#         StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#         StructField("_VALUE", StringType(), True)  # Element value
#     ]), True),
#
#     # IssuingAuthority element
#     StructField("IssuingAuthority", StructType([
#         StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
#         StructField("_VALUE", StringType(), True)  # Element value
#     ]), True),
#
#     # DocumentDate element with DatePeriod structure
#     StructField("DocumentDate", ArrayType(document_date_schema), True),
#
#     # IDRegDocumentMention element
#     StructField("IDRegDocumentMention", ArrayType(StructType([
#         StructField("_IDRegDocumentID", LongType(), True),  # Attribute: IDRegDocumentID
#         StructField("_ReferenceType", StringType(), True),  # Attribute: ReferenceType
#         StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
#     ])), True),
#
#     # FeatureVersionReference (reference element)
#     StructField("FeatureVersionReference", ArrayType(StringType()), True),
#
#     # DocumentedNameReference element
#     StructField("DocumentedNameReference", ArrayType(StructType([
#         StructField("_DocumentedNameID", LongType(), True),  # Attribute: DocumentedNameID
#         StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
#     ])), True),
#
#     # ProfileRelationshipReference (reference element)
#     StructField("ProfileRelationshipReference", ArrayType(StringType()), True)
# ])