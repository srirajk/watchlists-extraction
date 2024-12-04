import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, ArrayType
from collections import defaultdict

# Initialize Spark Session
appName = "OFAC_REFERENCE_DATA_EXAMPLE"
spark = SparkSession.builder \
    .master("local[1]") \
    .appName(appName) \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
    .getOrCreate()

# Define base path for XML file
source_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/source_data/ofac/"
xmlFilePath = source_data_base_path + "sdn_advanced.xml"

reference_data_base_path = "/Users/srirajkadimisetty/projects/spark-ofac-extraction/reference_data/ofac/"


# Define the output file path
output_file_path = f"{reference_data_base_path}/reference_values_map.json"

# Define the schema if known, otherwise Spark will infer it
# Reading XML Data
reference_val_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "ReferenceValueSets") \
    .load(xmlFilePath)

# Print schema and show a few rows for debugging
print("Loaded reference data schema:")
reference_val_df.printSchema()
print("Sample data from reference_val_df:")
reference_val_df.show(5, truncate=False)

# Function to convert DataFrame rows to a dictionary with ID as key
def convert_df_to_dict(df, id_field):
    result_dict = {}
    for row in df.collect():
        row_dict = row.asDict()
        # Use the specified ID field as the key in the dictionary
        row_id = row_dict.pop(id_field, None)
        if row_id is not None:
            result_dict[row_id] = row_dict
    return result_dict

# Function to process each reference value's DataFrame
def process_reference_values_df(df):
    map_of_maps = defaultdict(dict)

    # Iterate over each top-level field in the schema
    for field in df.schema.fields:
        print(f"Processing field: {field.name}")
        if isinstance(field.dataType, StructType):
            # Process nested fields in the struct
            for nested_field in field.dataType.fields:
                nested_field_name = nested_field.name
                if isinstance(nested_field.dataType, ArrayType):
                    # Explode the nested array
                    print(f"Field {nested_field_name} is an array inside {field.name}. Exploding it...")
                    exploded_df = df.select(explode(col(f"{field.name}.{nested_field_name}")).alias(nested_field_name))
                    exploded_df.printSchema()
                    exploded_df.show(5, truncate=False)

                    # Select individual fields from the struct
                    flattened_df = exploded_df.select(f"{nested_field_name}.*")

                    # If the exploded array contains an _ID field, convert to dict
                    if "_ID" in flattened_df.columns:
                        inner_map = convert_df_to_dict(flattened_df, "_ID")
                        map_of_maps[nested_field_name] = inner_map
                    else:
                        print(f"Field {nested_field_name} does not contain '_ID'. Skipping...")
                else:
                    print(f"Field {nested_field_name} inside {field.name} is not an array. Skipping...")
        else:
            print(f"Field {field.name} is not a struct. Skipping...")

    print("Finished processing reference values DataFrame.")
    return map_of_maps

# Process the reference data
print("Processing reference values...")
reference_values_map = process_reference_values_df(reference_val_df)

# Convert the reference_values_map to a JSON string
reference_values_json = json.dumps(reference_values_map, indent=2)



# Write the JSON string to the file
with open(output_file_path, "w") as json_file:
    json_file.write(reference_values_json)

print(f"Reference values map has been written to {output_file_path}")


# Print the resulting map
print("Reference values map:")
for field_name, inner_map in reference_values_map.items():
    print(f"Field: {field_name}")
    for row_id, node in inner_map.items():
        print(f"ID: {row_id}, Node: {json.dumps(node, indent=2)}")

spark.stop()