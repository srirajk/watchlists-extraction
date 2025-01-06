from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
import pandas as pd
import json

# Initialize Spark Session with Arrow enabled
spark = SparkSession.builder \
    .appName("ComplexNestedVectorizedUDF") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Sample Data with deeper nesting
data = [
    (1, "John", "Doe",
     {"home": {"street": "123 Main St", "city": "Anytown", "state": "CA", "zip": "12345"},
      "work": {"street": "456 Corp Ave", "city": "Biztown", "state": "NY", "zip": "67890"}},
     [{"type": "phone", "value": "123-456-7890", "is_primary": True, "details": {"country_code": "+1", "extension": "123"}},
      {"type": "email", "value": "john.doe@example.com", "is_primary": False, "details": {"domain": "example.com", "is_verified": True}}]
     ),
    (2, "Jane", "Smith",
     {"home": {"street": "789 Oak Ave", "city": "Somewhere", "state": "TX", "zip": "54321"},
      "work": {"street": "101 Biz St", "city": "Workville", "state": "CA", "zip": "98765"}},
     [{"type": "phone", "value": "987-654-3210", "is_primary": True, "details": {"country_code": "+1", "extension": "456"}},
      {"type": "email", "value": "jane.smith@example.com", "is_primary": True, "details": {"domain": "example.com", "is_verified": False}}]
     )
]

# Define Schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", MapType(StringType(), 
        StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True)
        ])
    ), True),
    StructField("contact_info", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("is_primary", StringType(), True),
            StructField("details", MapType(StringType(), StringType(), True))
        ])
    ), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
print("Input DataFrame:")
df.show(truncate=False)

# Define Vectorized UDF
@pandas_udf("struct<full_name:string, addresses:string, contact_info:string>")
def process_complex_data(first_name: pd.Series, last_name: pd.Series, address: pd.Series, contact_info: pd.Series) -> pd.DataFrame:
    def process_row(first_name, last_name, address, contact_info):
        full_name = f"{first_name} {last_name}"
        
        addresses = []
        for addr_type, addr in address.items():
            formatted_addr = f"{addr_type.upper()}: {addr['street']}, {addr['city']}, {addr['state']} {addr['zip']}"
            addresses.append(formatted_addr)
        formatted_addresses = "; ".join(addresses)
        
        contacts = []
        for contact in contact_info:
            details = ", ".join([f"{k}:{v}" for k, v in contact['details'].items()])
            formatted_contact = f"{contact['type'].upper()}: {contact['value']} (Primary: {contact['is_primary']}, {details})"
            contacts.append(formatted_contact)
        formatted_contacts = "; ".join(contacts)
        
        return {
            "full_name": full_name,
            "addresses": formatted_addresses,
            "contact_info": formatted_contacts
        }
    
    result = [process_row(fn, ln, addr, ci) for fn, ln, addr, ci in zip(first_name, last_name, address, contact_info)]
    return pd.DataFrame(result)

# Apply the UDF
result_df = df.select(
    process_complex_data(col("first_name"), col("last_name"), col("address"), col("contact_info")).alias("processed_data")
)

# Expand and Display Results
print("Output DataFrame:")
result_df.select("processed_data.*").show(truncate=False)

# Stop Spark Session
spark.stop()