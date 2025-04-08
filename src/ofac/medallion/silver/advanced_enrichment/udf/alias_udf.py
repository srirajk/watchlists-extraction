import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, ArrayType, MapType

class AliasUDFs:

    alias_return_type = ArrayType(
        StructType([
            StructField("is_primary", BooleanType(), True),
            StructField("is_low_quality", BooleanType(), True),
            StructField("alias_type_value", StringType(), True),
            StructField("documented_names", ArrayType(MapType(StringType(), StringType())), True)
        ])
    )

    @staticmethod
    def parse_string_to_dict(s):
        """
        Sample input: {'Script': '{scriptId=215, scriptValue={_VALUE=Latin, _ScriptCode=Latin}}',
                       'ScriptStatus': '{scriptStatusId=1, scriptStatusValue=Unknown}',
                       'Entity Name': 'COMERCIAL CIMEX, S.A.'}
        """
        try:
            # Use regex to handle nested structures
            pattern = r'(\w+)=(\{[^}]+\}|\w+)'
            return {k: v.strip('{}') for k, v in re.findall(pattern, s)}
        except:
            return {}


    @staticmethod
    @udf(returnType=alias_return_type)
    def transform_alias(aliases):
        if aliases is None:
            return None
        transformed_aliases = []
        for alias in aliases:
            transformed_documented_names = AliasUDFs.transform_documented_names(alias.documented_names)

            """
            for doc_name in alias.documented_names:
                transformed_doc_name = {}
                for key, value in doc_name.items():
                    if key == "Script":
                        script_dict = AliasUDFs.parse_string_to_dict(value)
                        script_value_dict = AliasUDFs.parse_string_to_dict(script_dict.get('scriptValue', '{}'))
                        transformed_doc_name[key] = script_value_dict.get('_VALUE', '')
                    elif key == "ScriptStatus":
                        script_status_dict = AliasUDFs.parse_string_to_dict(value)
                        transformed_doc_name[key] = script_status_dict.get('scriptStatusValue', '')
                    else:
                        transformed_doc_name[key] = value
                transformed_documented_names.append(transformed_doc_name)
            """
            
            transformed_aliases.append({
                "is_primary": alias.is_primary,
                "is_low_quality": alias.is_low_quality,
                "alias_type_value": alias.alias_type_value,
                "documented_names": transformed_documented_names
            })
        
        return transformed_aliases

    @staticmethod
    def transform_documented_names(documented_names):
        transformed_documented_names = []
        for doc_name in documented_names:
            transformed_doc_name = {}
            for key, value in doc_name.items():
                if key == "Script":
                    script_dict = AliasUDFs.parse_string_to_dict(value)
                    script_value_dict = AliasUDFs.parse_string_to_dict(script_dict.get('scriptValue', '{}'))
                    transformed_doc_name[key] = script_value_dict.get('_VALUE', '')
                elif key == "ScriptStatus":
                    script_status_dict = AliasUDFs.parse_string_to_dict(value)
                    transformed_doc_name[key] = script_status_dict.get('scriptStatusValue', '')
                else:
                    transformed_doc_name[key] = value
            transformed_documented_names.append(transformed_doc_name)
        return transformed_documented_names
