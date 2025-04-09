from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

enriched_date_period_schema = StructType([
    StructField("date", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("start_date_range", MapType(StringType(), StringType()), True),
    StructField("end_date_range", MapType(StringType(), StringType()), True),
    StructField("calendar_type", StringType(), True)
])

identification_schema = ArrayType(
    StructType([
        StructField("Type", StringType(), True),
        StructField("Value", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Dates", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("date_period", enriched_date_period_schema, True)
            ])
        ), True),
        StructField("Issued Location", MapType(StringType(), StringType()), True)
    ])
)

feature_schema = ArrayType(
    StructType([
        StructField("Type", StringType(), True),
        StructField("Date", ArrayType(enriched_date_period_schema), True),
        StructField("Value", StringType(), True),
        StructField("Location", MapType(StringType(), StringType()), True)
    ])
)

sanction_measure_schema = ArrayType(
    StructType([
        StructField("List", StringType(), True),
        StructField("Measures", ArrayType(
            StructType([
                StructField("Sanctions type", StringType(), True),
                StructField("Values", StringType(), True)
            ])
        ), True),
    ])
)


relations_schema = ArrayType(
    StructType([
        StructField("relation_quality", StringType(), True),
        StructField("relation_type", StringType(), True),
        StructField("to_profile_id", StringType(), True),
        StructField("date_period", enriched_date_period_schema, True),
        StructField("to_profile_documented_name", ArrayType(MapType(StringType(), StringType())), True)
    ])
)