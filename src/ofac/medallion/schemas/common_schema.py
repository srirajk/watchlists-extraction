from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, BooleanType, ArrayType, MapType
)

return_date_boundary_schema = StructType([
    StructField("fixed", StringType(), True),
    StructField("range", StructType([
        StructField("from", StringType(), True),
        StructField("to", StringType(), True)
    ]), True),
]
)

return_date_period_schema = StructType([
    StructField("start_date", return_date_boundary_schema, True),
    StructField("end_date", return_date_boundary_schema, True),
    StructField("calendar_type_id", StringType(), True),
    StructField("calendar_type_value", StringType(), True)
])