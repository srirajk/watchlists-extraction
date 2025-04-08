from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, MapType

from src.ofac.medallion.silver.advanced_enrichment.udf.common_udf import CommonUDFs
from src.ofac.medallion.silver.advanced_enrichment.udf.feature_udf import FeatureUDFs


class IdDocumentsUDFs:

    enriched_id_document_schema = ArrayType(
        StructType([
            StructField("document_type", StringType(), True),
            StructField("id_registration_number", StringType(), True),
            StructField("issuing_country", StringType(), True),
            StructField("validity_value", StringType(), True),
            StructField("dates", ArrayType(
                StructType([
                    StructField("type", StringType(), True),
                    StructField("date_period", CommonUDFs.enriched_date_period_schema, True)
                ])
            ), True),
            StructField("issued_in_location", FeatureUDFs.location_struct_type, True)
        ]))

    @staticmethod
    @udf(returnType=enriched_id_document_schema)
    def enrich_document(documents):
        if documents is None:
            return None
        enriched_documents = []
        for document in documents:
            d = {}
            document_type = document.document_type
            d["document_type"] = document_type

            if document.id_registration_number:
                d["id_registration_number"] = document.id_registration_number

            if document.issuing_country:
                d["issuing_country"] = document.issuing_country

            if document.validity_value:
                d["validity_value"] = document.validity_value

            if document.document_dates:
                dd_arr = []
                for dd in document.document_dates:
                    date_type = dd.document_date_type
                    date_period = CommonUDFs.enrich_date_period(dd.date_period)
                    dd_arr.append({
                        "type": date_type,
                        "date_period": date_period
                    })
                d["dates"] = dd_arr
            if document.issued_in_location is not None and len(document.issued_in_location) > 0:
                d["issued_in_location"] = FeatureUDFs.enrich_location(document.issued_in_location)

            enriched_documents.append(d)
        return enriched_documents