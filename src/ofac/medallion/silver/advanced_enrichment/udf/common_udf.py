from pyspark.sql.types import StructType, StructField, StringType, MapType




class CommonUDFs:

    enriched_date_period_schema = StructType([
        StructField("date", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("start_date_range", MapType(StringType(), StringType()), True),
        StructField("end_date_range", MapType(StringType(), StringType()), True),
        StructField("calendar_type", StringType(), True)
    ])

    @staticmethod
    def enrich_date_period(date_period):
        """
        Enrich the date period based on the given conditions.
        """
        if not date_period or 'start_date' not in date_period or 'end_date' not in date_period:
            return None
        print(f"date_period :: {date_period}")
        start_date = date_period.start_date
        end_date = date_period.end_date

        if start_date is None or end_date is None:
            return None

        print("STEP 1 PASSED")

        calendar_type_value = date_period.calendar_type_value

        # Check if either start_date or end_date is unknown
        if CommonUDFs.is_unknown(start_date) or CommonUDFs.is_unknown(end_date):
            return None

        print("STEP 2 PASSED")
        # Handle fixed dates
        if ('fixed' in start_date and 'fixed' in end_date and
                ( start_date['fixed'] is not None and  end_date['fixed'] is not None)):
            if start_date['fixed'] == end_date['fixed']:
                print(f"IN STEP 3 FIXED FIRST CONDITION")
                return {
                    "date": start_date['fixed'],
                    "calendar_type": calendar_type_value
                }
            else:
                print(f"IN STEP 3 FIXED SECOND CONDITION")
                return {
                    "start_date": start_date['fixed'],
                    "end_date": end_date['fixed'],
                    "calendar_type": calendar_type_value
                }
        print(f"date_range_input :: {date_period}")

        print("STEP 3 PASSED")
        # Handle range dates
        if 'range' in start_date and 'range' in end_date:
            start_date_range = start_date['range']
            end_date_range = end_date['range']
            if (start_date_range is not None and end_date_range is not None) and (start_date_range['from'] != 'Unknown' and start_date_range['to'] != 'Unknown' and \
                                                                                  end_date_range['from'] != 'Unknown' and end_date_range['to'] != 'Unknown'):
                value_ = {
                    "start_date_range": {
                        "from": start_date_range['from'],
                        "to": start_date_range['to']
                    },
                    "end_date_range": {
                        "from": end_date_range['from'],
                        "to": end_date_range['to']
                    },
                    "calendar_type": calendar_type_value
                }
                print(f"date_range_input :: {date_period} && date_range_output :: {value_}")
                return value_

        # If we reach here, it means we have an inconsistent state (e.g., one date is fixed, the other is range)
        return None

    @staticmethod
    def is_unknown(date):
        if 'fixed' in date:
            return date['fixed'] == 'Unknown'
        if 'range' in date:
            print(f"date_range_input :: {date}")
            return CommonUDFs.check_if_range_unknown(date)
        return True  # If neither 'fixed' nor 'range' is present, consider it unknown

    @staticmethod
    def check_if_range_unknown(date):
        return date['range'].get('from') == 'Unknown' and date['range'].get('to') == 'Unknown'



"""
Sample Input Records:

 Sample Request:


             {
             "start_date": {
               "fixed": "1974-12-11"
             },
             "end_date": {
               "fixed": "1974-12-11"
             },
             "calendar_type_id": "1",
             "calendar_type_value": "Gregorian"
           }

           {
             "start_date": {
               "fixed": "Unknown"
             },
             "end_date": {
               "fixed": "Unknown"
             },
             "calendar_type_id": "1",
             "calendar_type_value": "Gregorian"
           }


       {
           "start_date": {
             "range": {
               "from": "Unknown",
               "to": "Unknown"
             }
           },
           "end_date": {
             "range": {
               "from": "Unknown",
               "to": "Unknown"
             }
           },
           "calendar_type_id": "1",
           "calendar_type_value": "Gregorian"
         }
         
           {
           "start_date": {
             "range": {
               "from": "1974-12-11",
               "to": "1975-12-11"
             }
           },
           "end_date": {
             "range": {
               "from": "1977-12-11",
               "to": "1978-12-11"
             }
           },
           "calendar_type_id": "1",
           "calendar_type_value": "Gregorian"
         }


"""
