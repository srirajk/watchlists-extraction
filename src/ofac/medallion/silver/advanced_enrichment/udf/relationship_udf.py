from pycparser.c_ast import Struct
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, MapType

from src.ofac.medallion.silver.advanced_enrichment.udf.alias_udf import AliasUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.common_udf import CommonUDFs


class RelationshipUDFs:
    """
    This class contains UDFs for relationship enrichment.
    """

    enriched_relationships_schema = ArrayType(
        StructType([
            StructField("relation_quality", StringType(), True),
            StructField("relation_type", StringType(), True),
            StructField("to_profile_id", StringType(), True),
            StructField("date_period", CommonUDFs.enriched_date_period_schema, True),
            StructField("to_profile_documented_name", ArrayType(MapType(StringType(), StringType())), True)
        ])
    )

    @staticmethod
    @udf(returnType=enriched_relationships_schema)
    def enrich_relationships(relationships):
        """
        Get the relationships from the input.
        :param relationships: The relationships to get.
        :return: The relationships.
        """
        if relationships is None:
            return None

        r_arr = []
        for relationship in relationships:
            r = {
                "relation_quality": relationship.relation_quality,
                "relation_type": relationship.relation_type,
                "to_profile_id": relationship.to_profile_id,
                 }
            if relationship.date_period is not None:
                r["date_period"] = CommonUDFs.enrich_date_period(relationship.date_period)

            if relationship.to_profile_documented_name is not None and len(relationship.to_profile_documented_name) > 0:
                r["to_profile_documented_name"] = AliasUDFs.transform_documented_names(relationship.to_profile_documented_name)
            r_arr.append(r)

        return r_arr