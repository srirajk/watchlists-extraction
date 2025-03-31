from pyspark.sql.functions import udf, col, struct, collect_list, to_json, md5
from pyspark.sql.types import StringType, MapType

from src.ofac.custom_udfs import reference_data
from src.ofac.medallion.schemas.id_document_schema import id_document_schema


def get_id_reg_documents_df(spark, identities_df, locations_df):

    id_reg_documents_enriched_df = identities_df.withColumn(
        "transformed_document",
        enrich_id_document(struct(
            col("_ID"),
            col("_IDRegDocTypeID"),
            col("_IdentityID"),
            col("_IssuedBy-CountryID"),
            col("DocumentDate"),
            col("Comment"),
            col("IDRegistrationNo"),
            col("IssuingAuthority"),
            col("_IssuedIn-LocationID"),
            col("_ValidityID"),
        ))
    )

    # Explode and flatten the transformed data
    id_reg_documents_enriched_df = id_reg_documents_enriched_df.select(
        col("transformed_document.identity_id").alias("identity_id"),
        col("transformed_document.document_type").alias("document_type"),
        col("transformed_document.issuing_country").alias("issuing_country"),
        col("transformed_document.id_reg_document_id").alias("id_reg_document_id"),
        col("transformed_document.id_registration_number").alias("id_registration_number"),
        col("transformed_document.document_dates").alias("document_dates"),
        col("transformed_document.location_id").alias("issued_in_location_id"),
        col("transformed_document.validity_id").alias("validity_id")
    )

    # Join with Location df to enrich the issued_in_location_id
    id_reg_documents_enriched_df = id_reg_documents_enriched_df.join(
        locations_df,
        id_reg_documents_enriched_df["issued_in_location_id"] == locations_df["location_id"],
        "left"
    ).select(
        id_reg_documents_enriched_df["*"],
        struct(
            col("location_id").alias("id"),
            col("location_area").alias("area"),
            col("location_country").alias("country"),
            col("location_parts").alias("parts")
        ).alias("issued_in_location")
    ).drop("issued_in_location_id")

    id_documents_grouped_by_identity_df = id_reg_documents_enriched_df.groupBy("identity_id").agg(
        collect_list(
            struct(
                col("document_type"),
                col("issuing_country"),
                col("id_reg_document_id"),
                col("id_registration_number"),
                col("document_dates"),
                col("issued_in_location")
            )
        ).alias("id_documents"))

    id_documents_grouped_by_identity_df = (id_documents_grouped_by_identity_df
                                           .withColumn("documents_hash",
                                                 md5(to_json(col("id_documents")))))

    return id_documents_grouped_by_identity_df


#@udf(MapType(StringType(), StringType()))
@udf(id_document_schema)
def enrich_id_document(row):
    """
    Transform a single row of id_reg_documents_df into a POJO-like record.
    """

    # print(row)
    identity_id = row["_IdentityID"]
    location_id = row["_IssuedIn-LocationID"]
    validity_id = row["_ValidityID"]
    document_type = map_document_type(row["_IDRegDocTypeID"])
    issuing_country = map_issuing_country(row["_IssuedBy-CountryID"])
    id_reg_document_id = row["_ID"]
    id_registration_number = row["IDRegistrationNo"]._VALUE if row["IDRegistrationNo"] and "_VALUE" in row[
        "IDRegistrationNo"] else "Unknown"
    document_dates = []

    # Extract document dates if available
    if row["DocumentDate"]:
        for date in row["DocumentDate"]:
            doc_date_type_value = "Unknown"
            if date._IDRegDocDateTypeID:
                doc_date_type_id = date._IDRegDocDateTypeID
                doc_date_type_value = (reference_data.get("IDRegDocDateType", {})
                .get(str(doc_date_type_id), {}).get(
                    "_VALUE", "Unknown"))
            if date.DatePeriod:  # Ensure DatePeriod exists
                date_period = date.DatePeriod
                # Extract Start and End blocks
                start = date_period.Start if hasattr(date_period, "Start") and date_period.Start else None
                end = date_period.End if hasattr(date_period, "End") and date_period.End else None

                # Extract "From" and "To" values from Start
                start_from = start.From if start and hasattr(start, "From") else None
                start_to = start.To if start and hasattr(start, "To") else None

                # Extract "From" and "To" values from End
                end_from = end.From if end and hasattr(end, "From") else None
                end_to = end.To if end and hasattr(end, "To") else None

                # Determine if "Start" and "End" represent a single fixed date or a range
                start_date = {}
                if start_from and start_to and (
                        start_from.Year == start_to.Year
                        and start_from.Month == start_to.Month
                        and start_from.Day == start_to.Day
                ):  # Single fixed date
                    start_date["fixed"] = f"{start_from.Year}-{start_from.Month}-{start_from.Day}"
                else:  # Range
                    start_date["range"] = {
                        "from": f"{start_from.Year}-{start_from.Month}-{start_from.Day}" if start_from else "Unknown",
                        "to": f"{start_to.Year}-{start_to.Month}-{start_to.Day}" if start_to else "Unknown"
                    }

                end_date = {}
                if end_from and end_to and (
                        end_from.Year == end_to.Year
                        and end_from.Month == end_to.Month
                        and end_from.Day == end_to.Day
                ):  # Single fixed date
                    end_date["fixed"] = f"{end_from.Year}-{end_from.Month}-{end_from.Day}"
                else:  # Range
                    end_date["range"] = {
                        "from": f"{end_from.Year}-{end_from.Month}-{end_from.Day}" if end_from else "Unknown",
                        "to": f"{end_to.Year}-{end_to.Month}-{end_to.Day}" if end_to else "Unknown"
                    }

                document_dates.append({
                    "document_date_type": doc_date_type_value,
                    "date_period":
                        {
                            "start_date": start_date,
                            "end_date": end_date
                        }}
                )

    # Build the POJO-like record
    return {
        "identity_id": identity_id,
        "document_type": document_type,
        "issuing_country": issuing_country,
        "id_reg_document_id": id_reg_document_id,
        "id_registration_number": id_registration_number,
        "document_dates": document_dates,
        "location_id": location_id,
        "validity_id": validity_id,
    }


def map_document_type(doc_type_id):
    """
    Map the _IDRegDocTypeID to a human-readable document type using the reference map.
    """
    return reference_data.get("IDRegDocType", {}).get(str(doc_type_id), {}).get("_VALUE", "Unknown")


def map_issuing_country(country_id):
    """
    Map the _IssuedBy-CountryID to a country description and value using the reference map.
    """
    country_info = reference_data.get("AreaCode", {}).get(str(country_id), {})
    description = country_info.get("_Description", "Unknown")
    value = country_info.get("_VALUE", "Unknown")
    return f"{description} ({value})"
