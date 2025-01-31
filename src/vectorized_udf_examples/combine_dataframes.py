from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col
from functools import reduce

# Initialize Spark Session
spark = SparkSession.builder.appName("CombineDataFrames").getOrCreate()

# Create sample DataFrames
identifies_data = [
    (1, "John Doe", "1990-01-01"),
    (2, "Jane Smith", "1985-05-15")
]
identifies_df = spark.createDataFrame(identifies_data, ["id", "name", "dob"])

securities_data = [
    (1, "AAPL", 100),
    (2, "GOOGL", 50)
]
securities_df = spark.createDataFrame(securities_data, ["id", "symbol", "quantity"])

other_data = [
    (1, "US", "English"),
    (2, "UK", "English")
]
other_df = spark.createDataFrame(other_data, ["id", "country", "language"])

# Rename columns to ensure uniqueness
identifies_df = identifies_df.select([col(c).alias(f"identifies_{c}") for c in identifies_df.columns])
securities_df = securities_df.select([col(c).alias(f"securities_{c}") for c in securities_df.columns])
other_df = other_df.select([col(c).alias(f"other_{c}") for c in other_df.columns])

# List of DataFrames with their field names
dataframes = [
    ("identifies", identifies_df),
    ("securities", securities_df),
    ("other", other_df)
]

# Combine DataFrames into a single DataFrame with nested fields
final_df = reduce(
    lambda acc, item: acc.join(item[1], acc[f"identifies_id"] == item[1][f"{item[0]}_id"], "left")
                         .drop(f"{item[0]}_id")
                         .withColumn(item[0], struct(*[col(c) for c in item[1].columns if c != f"{item[0]}_id"])),
    dataframes[1:],
    dataframes[0][1]
)

# Show the result
print("Combined DataFrame:")
final_df.show(truncate=False)

# Optional: Write the result to a file or table
# final_df.write.mode("overwrite").parquet("path/to/output")

# Stop Spark Session
spark.stop()