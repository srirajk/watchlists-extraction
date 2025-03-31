from pyspark.sql.functions import udf

from src.ofac.custom_udfs import get_reference_value
from src.ofac.medallion.schemas.common_schema import return_date_period_schema


@udf(return_date_period_schema)
def parse_date_period_udf(date_period):

    if date_period is None:
        # Return a default structure with empty/null values
        return {
            "calendar_type_id": None,
            "calendar_type_value": None,
            "start_date": {},
            "end_date": {},
        }

    return parse_date_period(date_period)

def parse_date_period(date_period):

    calendar_type_id = date_period["_CalendarTypeID"]
    calendar_type_value = get_reference_value("CalendarType", calendar_type_id)

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

    return {
        "calendar_type_id": calendar_type_id,
        "calendar_type_value": calendar_type_value,
        "start_date": start_date,
        "end_date": end_date,
    }

def parse_date(date):
    year = date["Year"]
    month = date["Month"]
    day = date["Day"]
    calendar_type_id = date["_CalendarTypeID"]
    calendar_type_value = get_reference_value("CalendarType", calendar_type_id)

    return {
        "year": year,
        "month": month,
        "day": day,
        "calendar_type_id": calendar_type_id,
        "calendar_type_value": calendar_type_value
    }