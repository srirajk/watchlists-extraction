"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, to_json, monotonically_increasing_id, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
import pandas as pd
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexDataProcessing").getOrCreate()

# Define the input schema
input_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("addressDetails", StructType([
        StructField("addresses", ArrayType(StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True)
        ])), True)
    ]), True),
    StructField("contact_info", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("is_primary", StringType(), True)
        ])
    ), True)
])

# Create sample data
data = [
    (1, "John", "Doe", 
     {"addresses": [
         {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"},
         {"street": "456 Oak Rd", "city": "Sometown", "state": "CA", "zip": "67890"}
     ]},
     [{"type": "phone", "value": "123-456-7890", "is_primary": "true"},
      {"type": "email", "value": "john.doe@example.com", "is_primary": "false"}]),
    (2, "Jane", "Smith", 
     {"addresses": [
         {"street": "789 Elm St", "city": "Othertown", "state": "NY", "zip": "10111"}
     ]},
     [{"type": "phone", "value": "987-654-3210", "is_primary": "true"},
      {"type": "email", "value": "jane.smith@example.com", "is_primary": "true"}])
]


# Create DataFrame
df = spark.createDataFrame(data, schema=input_schema)

# Define output schema
output_schema = MapType(
    StringType(),
    StructType([
        StructField("full_name", StringType(), True),
        StructField("addresses", ArrayType(StringType()), True)
    ])
)

# Define Vectorized UDF
@pandas_udf(output_schema)
def process_complex_data(first_name: pd.Series, last_name: pd.Series, address_details: pd.Series, contact_info: pd.Series) -> pd.Series:
    def process_row(first_name, last_name, address_details, contact_info):
        try:
            full_name = f"{first_name} {last_name}"
            
            address_dict = json.loads(address_details)
            address_list = address_dict['addresses']
            formatted_addresses = [f"{addr['street']}, {addr['city']}, {addr['state']} {addr['zip']}" for addr in address_list]
            
            result = {
                f"record_{i}": {"full_name": full_name, "addresses": formatted_addresses}
                for i in range(1, 4)  # Generate 3 records
            }
            
            return result
        except Exception as e:
            # Log the error and return an empty dict
            print(f"Error processing row: {str(e)}")
            return {}
    
    return pd.Series([process_row(fn, ln, ad, ci) for fn, ln, ad, ci in zip(first_name, last_name, address_details, contact_info)])

# Apply the UDF and add an id column
result_df = df.select(
    col("id"),
    process_complex_data(
        col("first_name"),
        col("last_name"),
        to_json(col("addressDetails")),
        to_json(col("contact_info"))
    ).alias("processed_data")
)

# Add a new unique id column
result_df = result_df.withColumn("new_id", monotonically_increasing_id())

# Display input DataFrame
print("Input DataFrame:")
df.show(truncate=False)

# Display output DataFrame
print("\nOutput DataFrame:")
result_df.show(truncate=False)

# Accessing specific records
print("\nAccessing specific records:")
result_df.select(
    col("id"),
    col("new_id"),
    col("processed_data")["record_1"].alias("record_1"),
    col("processed_data")["record_2"].alias("record_2"),
    col("processed_data")["record_3"].alias("record_3")
).show(truncate=False)

# Stop Spark Session
spark.stop()

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, to_json, monotonically_increasing_id, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
import pandas as pd
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexDataProcessing").getOrCreate()

# Define the input schema
input_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("addressDetails", StructType([
        StructField("addresses", ArrayType(StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True)
        ])), True)
    ]), True),
    StructField("contact_info", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("is_primary", StringType(), True)
        ])
    ), True)
])

# Create sample data
data = [
    (1, "John", "Doe", 
     {"addresses": [
         {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"},
         {"street": "456 Oak Rd", "city": "Sometown", "state": "CA", "zip": "67890"}
     ]},
     [{"type": "phone", "value": "123-456-7890", "is_primary": "true"},
      {"type": "email", "value": "john.doe@example.com", "is_primary": "false"}]),
    (2, "Jane", "Smith", 
     {"addresses": [
         {"street": "789 Elm St", "city": "Othertown", "state": "NY", "zip": "10111"}
     ]},
     [{"type": "phone", "value": "987-654-3210", "is_primary": "true"},
      {"type": "email", "value": "jane.smith@example.com", "is_primary": "true"}])
]

# Create DataFrame
df = spark.createDataFrame(data, schema=input_schema)

# Define output schema
output_schema = MapType(
    StringType(),
    StructType([
        StructField("full_name", StringType(), True),
        StructField("addresses", ArrayType(StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("country", StringType(), True)
        ])), True)
    ])
)

# Define Vectorized UDF
@pandas_udf(output_schema)
def process_complex_data(first_name: pd.Series, last_name: pd.Series, address_details: pd.Series, contact_info: pd.Series) -> pd.Series:
    def process_row(first_name, last_name, address_details, contact_info):
        try:
            full_name = f"{first_name} {last_name}"
            
            address_dict = json.loads(address_details)
            address_list = address_dict['addresses']
            formatted_addresses = [
                {
                    "street": addr['street'],
                    "city": addr['city'],
                    "state": addr['state'],
                    "zip": addr['zip'],
                    "country": "USA"  # Assuming USA as a default country
                } for addr in address_list
            ]
            
            result = {
                f"record_{i}": {"full_name": full_name, "addresses": formatted_addresses}
                for i in range(1, 4)  # Generate 3 records
            }
            
            return result
        except Exception as e:
            print(f"Error processing row: {str(e)}")
            return {}
    
    return pd.Series([process_row(fn, ln, ad, ci) for fn, ln, ad, ci in zip(first_name, last_name, address_details, contact_info)])

# Apply the UDF and add an id column
result_df = df.select(
    col("id"),
    process_complex_data(
        col("first_name"),
        col("last_name"),
        to_json(col("addressDetails")),
        to_json(col("contact_info"))
    ).alias("processed_data")
)

# Add a new unique id column
result_df = result_df.withColumn("new_id", monotonically_increasing_id())

# Display input DataFrame
print("Input DataFrame:")
df.show(truncate=False)

# Display output DataFrame
print("\nOutput DataFrame:")
result_df.show(truncate=False)

# Accessing specific records
print("\nAccessing specific records:")
result_df.select(
    col("id"),
    col("new_id"),
    col("processed_data")["record_1"].alias("record_1"),
    col("processed_data")["record_2"].alias("record_2"),
    col("processed_data")["record_3"].alias("record_3")
).select(
    "id",
    "new_id",
    "record_1.full_name",
    "record_1.addresses",
    "record_2.full_name",
    "record_2.addresses",
    "record_3.full_name",
    "record_3.addresses"
).show(truncate=False)

# Stop Spark Session
spark.stop()