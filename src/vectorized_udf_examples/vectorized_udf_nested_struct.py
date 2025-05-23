from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import pandas as pd
import json

# Initialize Spark Session with Arrow enabled
spark = SparkSession.builder \
    .appName("ComplexNestedVectorizedUDF") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Define Schema (unchanged)
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True),
    StructField("contact_info", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("is_primary", StringType(), True)
        ])
    ), True)
])

output_schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("formatted_address", StringType(), True),
    StructField("formatted_contact_info", StringType(), True)
])

# Sample data (unchanged)
data = [
    (1, "John", "Doe", 
     {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"},
     [{"type": "phone", "value": "123-456-7890", "is_primary": "true"},
      {"type": "email", "value": "john.doe@example.com", "is_primary": "false"}]),
    (2, "Jane", "Smith", 
     {"street": "456 Elm St", "city": "Othertown", "state": "NY", "zip": "67890"},
     [{"type": "phone", "value": "987-654-3210", "is_primary": "true"},
      {"type": "email", "value": "jane.smith@example.com", "is_primary": "true"}])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)
print("Input DataFrame:")
df.show(truncate=False)




# Define Vectorized UDF
#@pandas_udf("struct<full_name:string, formatted_address:string, formatted_contact_info:string>")
@pandas_udf(output_schema)
def process_complex_data(first_name: pd.Series, last_name: pd.Series, address: pd.Series, contact_info: pd.Series) -> pd.DataFrame:
    def process_row(first_name, last_name, address, contact_info):
        full_name = f"{first_name} {last_name}"
        
        # Parse the address string into a dictionary
        address_dict = json.loads(address)
        formatted_address = f"{address_dict['street']}, {address_dict['city']}, {address_dict['state']} {address_dict['zip']}"
        
        # Parse the contact_info string into a list of dictionaries
        contacts = json.loads(contact_info)
        formatted_contacts = []
        for contact in contacts:
            formatted_contact = f"{contact['type'].upper()}: {contact['value']} (Primary: {contact['is_primary']})"
            formatted_contacts.append(formatted_contact)
        formatted_contacts_str = "; ".join(formatted_contacts)
        
        return {
            "full_name": full_name,
            "formatted_address": formatted_address,
            "formatted_contact_info": formatted_contacts_str
        }
    
    result = [process_row(fn, ln, addr, ci) for fn, ln, addr, ci in zip(first_name, last_name, address, contact_info)]
    return pd.DataFrame(result)

# Apply the UDF
result_df = df.select(
    process_complex_data(
        col("first_name"),
        col("last_name"),
        to_json(col("address")),
        to_json(col("contact_info"))
    ).alias("processed_data")
)

# Expand and Display Results
print("Output DataFrame:")
result_df.select("processed_data.*").show(truncate=False)

# Stop Spark Session
spark.stop()