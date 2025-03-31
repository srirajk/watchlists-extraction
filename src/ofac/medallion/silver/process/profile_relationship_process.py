
from pyspark.sql.functions import col, explode, udf, struct, collect_list, to_json, md5
from pyspark.sql.types import StringType

from src.ofac.custom_udfs import get_reference_value
from src.ofac.medallion.silver.process.common_process import parse_date_period_udf


def get_profile_relationships_by_profile_id(profiles_df, profile_relationships_df):

    primary_profiles_df = profiles_df.select(
            col("profile_id"),
            explode("aliases").alias("alias")
        ).filter(
            col("alias.is_primary") == True
         ).select(
            col("profile_id"),
            col("alias.documented_names").alias("to_profile_documented_name")
        )

    profile_relationships_df = profile_relationships_df.select(
        col("_ID").alias("relationship_id"),
        col("_From-ProfileID").alias("from_profile_id"),
        col("_To-ProfileID").alias("to_profile_id"),
        col("_RelationTypeID").alias("relation_type_id"),
        col("_RelationQualityID").alias("relation_quality_id"),
        col("_SanctionsEntryID").alias("sanctions_entry_id"),
        col("_Former").alias("former"),
        col("Comment").alias("comment"),
        #col("DatePeriod").alias("date_period_new"),
        #col("IDRegDocumentReference").alias("id_document_reference"),
        get_relation_quality(col("_RelationQualityID")).alias("relation_quality"),
        get_relation_type(col("_RelationTypeID")).alias("relation_type"),
        parse_date_period_udf(col("DatePeriod")).alias("date_period"),
    )

    profile_relationships_df = profile_relationships_df.join(
        primary_profiles_df,
        profile_relationships_df["to_profile_id"] == primary_profiles_df["profile_id"],
        "left"
    ).drop(primary_profiles_df["profile_id"])

    value_columns = [c for c in profile_relationships_df.columns if c != "from_profile_id"]

    profile_relationships_df_by_from_profile_id = profile_relationships_df.groupBy("from_profile_id").agg(
        collect_list(struct(*[col(c) for c in value_columns])).alias("relationships")
    )

    profile_relationships_df_by_profile_id = (profile_relationships_df_by_from_profile_id
                                                   .withColumn("relationships_hash",
                                                        md5(to_json(col("relationships")))))

    return profile_relationships_df_by_profile_id






@udf(StringType())
def get_relation_quality(relation_quality_id):
    """
    Get the human-readable value of a relation quality ID from the reference data.
    """
    print
    return get_reference_value("RelationQuality", relation_quality_id)

@udf(StringType())
def get_relation_type(relation_type_id):
    """
    Get the human-readable value of a relation type ID from the reference data.
    """
    return get_reference_value("RelationType", relation_type_id)