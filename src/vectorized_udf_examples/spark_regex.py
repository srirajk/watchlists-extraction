from pyspark.sql import SparkSession
from pyspark.sql.functions import split, collect_list, struct, regexp_extract, regexp_replace, explode, monotonically_increasing_id, trim, array_distinct

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TransformFurtherInformation") \
    .getOrCreate()

# Create a sample DataFrame
data = [
    ("1", """[AUSTRALIA SANCTIONS - DFAT]No 275, a, b, c, d, e, f (Sep 2022 - amended). PRIMARY NAME: Gama’a al Islamiyya. a.k.a.: Al Gama’at; Egyptian Al’Gama’at al Islamiyya; GI; IG; Islamic Gama’at; Islamic Group. Address: Egypt. Additional Information: Gama’a al Islamiyya is an Egyptian Sunni Islamist movement.[CANADA SANCTIONS - CCC]Jul 2022 - addition. Jun 2024 - amended. PRIMARY NAME: Al-Gama’a al-Islamiyya (AGAI). Also known as Islamic Group, (IG). Description: Al-Gama’a al-Islamiyya (AGAI) started in the early 1970s as an Islamist student movement on Egyptian campuses. By the late 1970s, the organization began to advocate change by force. Its primary goal was to overthrow the Egyptian government and replace it with an Islamic state governed by Sharia Law. Past AGAI attacks primarily targeted the police, government officials, informants, government sympathizers, foreign tourists, and Coptic Christians. AGAI claimed responsibility for the February 1993 bombing of a Café in Cairo which killed at least 3 people and wounded 15 others, including a Canadian. AGAI was also responsible for the killing of 58 foreign tourists and four Egyptians in Luxor, Egypt, in November 1997.[USA SANCTIONS - OFAC]EC 2580/2001. CP 201/931/CFSP (Jun 2019 - annulled in relation to Case T-643/16. Jul 2024 - amended). PRIMARY NAME: “Gama’a al-Islamiyya” (a.k.a. “Al-Gama’a al-Islamiyya”) (“Islamic Group” - “IG”).[USA SANCTIONS - OFAC]Nov 2001 - addition. Jan 2021 - amended. Regime: Counter-Terrorism (International). PRIMARY NAME: AL-GAMAA AL-ISLAMIYYA. AKA: AL JAMA’A AL-ISLAMIYYA; AL-JAMA’AH AL-ISLAMIYYAH; GI; IG; THE ISLAMIC GROUP.Other Information: (UK Sanctions List Ref): CTI0030 (UK Statement of Reasons): Al-Gama’a al-Islamiyya (GI) is an Egyptian Sunni terrorist organisation who were responsible for a number of terrorist attacks in the 1980s and 1990s. Group ID: 6988.""")
]

df = spark.createDataFrame(data, ["id", "further_information"])

# Transform the data
transformed_df_1 = (df.withColumn(
    "sections",
    split("further_information", "(?=\\[)")
).withColumn(
    "section_id",
    monotonically_increasing_id()
))

print("******** transformed_df_1 *********")
transformed_df_1.show(truncate=False)
print("******** transformed_df_1 *********")


transformed_df_2 = (transformed_df_1.withColumn(
    "section",
    explode("sections")
).where(
    trim("section") != ""
))

print("******** transformed_df_2 *********")
transformed_df_2.show(truncate=False)
print("******** transformed_df_2 *********")

transformed_df_3 = (transformed_df_2.select(
    "id",
    "section_id",
    trim(regexp_extract("section", r'\[(.*?)\]', 1)).alias("key"),
    trim(regexp_replace("section", r'^\[.*?\]', '')).alias("value")
))

print("******** transformed_df_3 *********")
transformed_df_3.show(truncate=False)
print("******** transformed_df_3 *********")

transformed_df_4 = (    transformed_df_3.groupBy("id", "key").agg(
    collect_list("value").alias("values")
))

print("******** transformed_df_4 *********")
transformed_df_4.show(truncate=False)
print("******** transformed_df_4 *********")

transformed_df = (transformed_df_4.groupBy("id").agg(
    collect_list(
        struct("key", "values")
    ).alias("further_information_parsed")
))

print("******** transformed_df *********")
transformed_df.show(truncate=False)
print("******** transformed_df *********")

# Show the result
transformed_df.select("id", "further_information_parsed").show(truncate=False)

# Stop the SparkSession
spark.stop()