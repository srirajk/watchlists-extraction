from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("CombineDataFrames").getOrCreate()

# Create sample DataFrames
identifies_data = [
    (1, "John Doe", "1990-01-01"),
    (2, "Jane Smith", "1985-05-15"),
    (3, "Bob Johnson", "1978-12-10")
]
identifies_df = spark.createDataFrame(identifies_data, ["id", "name", "dob"])

securities_data = [
    (1, "AAPL", 100),
    (2, "GOOGL", 50),
    (4, "MSFT", 75)
]
securities_df = spark.createDataFrame(securities_data, ["id", "symbol", "quantity"])

other_data = [
    (1, "US", "English"),
    (2, "UK", "English"),
    (5, "CA", "French")
]
other_df = spark.createDataFrame(other_data, ["id", "country", "language"])

# List of DataFrames
dataframes = [identifies_df, securities_df, other_df]

# Start with the first DataFrame
final_df = dataframes[0]

# Iteratively join the remaining DataFrames
for df in dataframes[1:]:
    final_df = final_df.join(df, on="id", how="outer")

# Show the result
print("Combined DataFrame:")
final_df.show(truncate=False)

# Optional: Write the result to a file or table
# final_df.write.mode("overwrite").parquet("path/to/output")

# Stop Spark Session
spark.stop()