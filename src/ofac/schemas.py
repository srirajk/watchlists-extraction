from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType

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
            StructField("FeatureVersion", StructType([
                StructField("_ReliabilityID", LongType(), True),
                StructField("_ID", LongType(), True),
                StructField("Comment", StringType(), True),
                StructField("VersionLocation", StructType([
                    StructField("_LocationID", LongType(), True)
                ]))
            ])),
            StructField("IdentityReference", StructType([
                StructField("_IdentityID", LongType(), True),
                StructField("_IdentityFeatureLinkTypeID", LongType(), True)
            ]))
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