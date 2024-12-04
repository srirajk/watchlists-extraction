from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("OFAC_PARSING") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
    .getOrCreate()

# Paths to data
base_path = "/Users/srirajkadimisetty/projects/ofac-model-spark-demo/ofac-resources/"
distinct_party_xml = base_path + "sdn_advanced.xml"
reference_values_json = "/Users/srirajkadimisetty/projects/spark-python-demo/output/reference_values_map.json"

# Load reference values from JSON to a dictionary
with open(reference_values_json, 'r') as file:
    reference_data = json.load(file)

# Broadcast each reference dictionary for quick lookup
reference_maps = {key: spark.sparkContext.broadcast(value) for key, value in reference_data.items()}

# General UDF to lookup value from any reference map based on type and ID
def generalized_lookup(id, ref_type):
    ref_map = reference_maps.get(ref_type).value
    return ref_map.get(str(id), {}).get("_VALUE", "Unknown") if ref_map else "Unknown"

# Register UDF for reference lookup
from pyspark.sql.functions import udf
lookup_udf = udf(generalized_lookup, StringType())

# Define schema to match the structure of DistinctParty
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

# Read XML with schema
distinct_party_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "DistinctParty") \
    .schema(distinct_party_schema) \
    .load(distinct_party_xml)

distinct_party_df.show(truncate=False)


# Explode the "Profile" array to flatten the structure
exploded_profiles_df = distinct_party_df.select(
    col("_FixedRef"),
    explode(col("Profile")).alias("Profile")
)



# Function to process a single Profile row
def process_profile_row(profile_row):

    profile_id = profile_row["_ID"]

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
                    "documented_names": documented_names
                })

    return results

# Register the UDF
process_udf = udf(
    lambda profile_row: process_profile_row(profile_row),
    ArrayType(
        StructType([
            StructField("profile_id", LongType(), True),
            StructField("identity_id", LongType(), True),
            StructField("alias_type_value", StringType(), True),
            StructField("_Primary", BooleanType(), True),
            StructField("documented_names", ArrayType(MapType(StringType(), StringType())))
        ])
    )
)

# Apply the UDF to process each Profile row
processed_df = (
    exploded_profiles_df
    .withColumn("processed_data", process_udf(col("Profile")))
    .select(explode(col("processed_data")).alias("processed_row"))
)

# Flatten the exploded data into columns
flattened_df = processed_df.select(
    col("processed_row.profile_id").alias("profile_id"),
    col("processed_row.identity_id").alias("identity_id"),
    col("processed_row.alias_type_value").alias("alias_type_value"),
    col("processed_row._Primary").alias("is_primary"),
    col("processed_row.documented_names").alias("documented_names")
)

# Show the new DataFrame
flattened_df.show(truncate=False)



# Save the output to a JSON file
output_path = "/output/processed_data/"
flattened_df.write.mode("overwrite").json(output_path)


# Define the schema for DateBoundarySchemaType
date_boundary_schema = StructType([
    StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
    StructField("_BoundaryQualifierTypeID", LongType(), True),  # Attribute: BoundaryQualifierTypeID
    StructField("_VALUE", StringType(), True)  # Element value (actual date string)
])

# Define the schema for DurationSchemaType
duration_schema = StructType([
    StructField("Years", StructType([
        StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
        StructField("_VALUE", LongType(), True)  # Element value: Years
    ]), True),
    StructField("Months", StructType([
        StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
        StructField("_VALUE", LongType(), True)  # Element value: Months
    ]), True),
    StructField("Days", StructType([
        StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
        StructField("_VALUE", LongType(), True)  # Element value: Days
    ]), True),
    StructField("_Approximate", BooleanType(), True),  # Attribute: Approximate
    StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
])

# Define the schema for DatePeriod
date_period_schema = StructType([
    StructField("Comment", StringType(), True),  # Optional Comment
    StructField("Start", date_boundary_schema, True),  # Start date (DateBoundarySchemaType)
    StructField("End", date_boundary_schema, True),  # End date (DateBoundarySchemaType)
    StructField("DurationMinimum", duration_schema, True),  # Nested DurationMinimum
    StructField("DurationMaximum", duration_schema, True),  # Nested DurationMaximum
    StructField("_CalendarTypeID", LongType(), True),  # Attribute: CalendarTypeID
    StructField("_YearFixed", BooleanType(), True),  # Attribute: YearFixed
    StructField("_MonthFixed", BooleanType(), True),  # Attribute: MonthFixed
    StructField("_DayFixed", BooleanType(), True),  # Attribute: DayFixed
    StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
])

# Update the DocumentDate schema to include DatePeriod with DurationSchemaType and DateBoundarySchemaType
document_date_schema = StructType([
    StructField("DatePeriod", ArrayType(date_period_schema), True),  # Nested DatePeriod structure
    StructField("_IDRegDocDateTypeID", LongType(), True),  # Attribute: IDRegDocDateTypeID
    StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
])

# Define the complete schema for <IDRegDocuments>
id_reg_documents_schema = StructType([
    StructField("_ID", LongType(), True),  # Attribute: ID
    StructField("_IDRegDocTypeID", LongType(), True),  # Attribute: IDRegDocTypeID
    StructField("_IdentityID", LongType(), True),  # Attribute: IdentityID
    StructField("_IssuedBy-CountryID", LongType(), True),  # Attribute: IssuedBy-CountryID
    StructField("_IssuedIn-LocationID", LongType(), True),  # Attribute: IssuedIn-LocationID
    StructField("_ValidityID", LongType(), True),  # Attribute: ValidityID
    StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction

    # Comment element
    StructField("Comment", StringType(), True),

    # IDRegistrationNo element
    StructField("IDRegistrationNo", StructType([
        StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
        StructField("_VALUE", StringType(), True)  # Element value
    ]), True),

    # IssuingAuthority element
    StructField("IssuingAuthority", StructType([
        StructField("_DeltaAction", StringType(), True),  # Attribute: DeltaAction
        StructField("_VALUE", StringType(), True)  # Element value
    ]), True),

    # DocumentDate element with DatePeriod structure
    StructField("DocumentDate", ArrayType(document_date_schema), True),

    # IDRegDocumentMention element
    StructField("IDRegDocumentMention", ArrayType(StructType([
        StructField("_IDRegDocumentID", LongType(), True),  # Attribute: IDRegDocumentID
        StructField("_ReferenceType", StringType(), True),  # Attribute: ReferenceType
        StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
    ])), True),

    # FeatureVersionReference (reference element)
    StructField("FeatureVersionReference", ArrayType(StringType()), True),

    # DocumentedNameReference element
    StructField("DocumentedNameReference", ArrayType(StructType([
        StructField("_DocumentedNameID", LongType(), True),  # Attribute: DocumentedNameID
        StructField("_DeltaAction", StringType(), True)  # Attribute: DeltaAction
    ])), True),

    # ProfileRelationshipReference (reference element)
    StructField("ProfileRelationshipReference", ArrayType(StringType()), True)
])

id_reg_documents_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "IDRegDocument") \
    .schema(id_reg_documents_schema) \
    .load(distinct_party_xml)

# Show the DataFrame
id_reg_documents_df.show(truncate=False)

# Print the schema
id_reg_documents_df.printSchema()




# Perform the join on the Identity ID columns using a left join
joined_df = flattened_df.join(
    id_reg_documents_df,
    flattened_df["identity_id"] == id_reg_documents_df["_IdentityID"],
    "outer"  # Retain all rows from flattened_df, even if there's no match in id_reg_documents_df
)

# Select relevant columns after the join
result_df = joined_df.select(
    flattened_df["profile_id"],
    flattened_df["identity_id"],
    flattened_df["alias_type_value"],
    flattened_df["is_primary"],
    flattened_df["documented_names"],
    id_reg_documents_df["_IdentityID"].alias("id_identity_id"),  # Add columns from id_reg_documents_df
    id_reg_documents_df["_ID"].alias("id_reg_document_id"),  # Add columns from id_reg_documents_df
    id_reg_documents_df["_IDRegDocTypeID"].alias("id_reg_document_type_id"), # Add columns from id_reg_documents_df
    id_reg_documents_df["IDRegistrationNo"],
    id_reg_documents_df["IssuingAuthority"],
    id_reg_documents_df["DocumentDate"]
)

# Show the result
result_df.show(truncate=False)

# Define the output file path
output_file_path = "/output/result_df_joined_left/"

# Write the JSON string to the file
result_df.write.mode("overwrite").json(output_file_path)

result_df_count = result_df.count()
print(f"Result Count ::  {result_df_count}")


# how to check if any result_df has empty id_reg_document_id docs.. .. wqould just like the count.
# Count the number of rows with empty IDRegDocumentID
non_empty_id_reg_document_id_count = result_df.where(col("id_identity_id").isNotNull()).count()

print("Non Empty IDRegDocumentID Count ::  "+str(non_empty_id_reg_document_id_count))

empty_results = result_df_count - non_empty_id_reg_document_id_count

print("Empty IDRegDocumentID Count ::  "+str(empty_results))


print("flattened_df Count ::  "+str(flattened_df.count()))
print("IDRegDocuments Count ::  "+str(id_reg_documents_df.count()))


#result_df.printSchema()

result_df_not_mapped_identities_df = result_df.where(col("identity_id").isNotNull())
result_df_not_mapped_identities_df.show(truncate=False)








#
#
#
# # Convert the first 5 rows of the DataFrame to JSON strings
# json_list = distinct_party_df.toJSON().take(5)  # Limit to the first 5 rows
#
# # # Convert the JSON strings to Python dictionaries and print them
# clean_json_list = [json.loads(row) for row in json_list]
#
#
# # Convert the reference_values_map to a JSON string
# sample_raw_values_json_dump = json.dumps(clean_json_list, indent=2)
#
# print(sample_raw_values_json_dump)
#


# Stop Spark session
spark.stop()