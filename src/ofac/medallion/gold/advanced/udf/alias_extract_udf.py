from types import NoneType

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, MapType, StringType

from src.ofac.medallion.gold.advanced.schema.common import identification_schema, feature_schema, \
    sanction_measure_schema, relations_schema


class AliasGoldUDFs:

    @staticmethod
    @udf(returnType=relations_schema)
    def extract_relations(relations):
        return relations

    @staticmethod
    @udf(returnType=ArrayType(MapType(StringType(), StringType())))
    def extract_names(aliases):
        if aliases is None:
            return None
        names = []
        for alias in aliases:
            for dn in alias.documented_names:
                dn_name = {}
                for key, value in dn.items():
                    if key not in ["ScriptStatus"]:
                        dn_name[key] = value
                dn_name["category"] = "strong "if alias.is_low_quality else "weak"
                dn_name["primary"] = alias.is_primary
                names.append(dn_name)
        return names


    @staticmethod
    @udf(returnType=feature_schema)
    def extract_features(features):
        if features is None:
            return None
        f_arr = []
        for feature in features:
            if feature.type != "Location":
                f = {"Type": feature.type}
                #print(f"Processing feature: {feature.feature_versions}")  # Placeholder for actual transformation logic
                fv = feature.feature_versions[0]
                version = fv.versions[0]
                if version.detail_type == "DATE":
                    f["Date"] = fv.date_period
                elif version.detail_reference != "Unknown":
                    f["Value"] = version.detail_reference
                elif version.value:
                    f["Value"] = version.value
                elif fv.locations:
                    loc = fv.locations[0]
                    add = AliasGoldUDFs.get_address(loc)
                    f["Location"] = add
                #print(f"Feature: {f}")  # Placeholder for actual transformation logic
                f_arr.append(f)
        return f_arr


    @staticmethod
    @udf(returnType=sanction_measure_schema)
    def extract_sanction_measures(sanction_entries):
        if sanction_entries is None:
            return None
        sanctions = []
        for se in sanction_entries:
            sanction = {"List": se.list_value}
            measures = []
            for smRow in se.sanctions_measures:
                sm = {"Sanctions type": smRow.sanctions_type_value}
                if smRow.comments is not None and len(smRow.comments) > 0:
                    sm["Values"] = "; ".join(smRow.comments)
                measures.append(sm)
            sanction["Measures"] = measures
            sanctions.append(sanction)
        return sanctions

    @staticmethod
    @udf(returnType=identification_schema)
    def extract_identifications(profile_id, id_documents):
        if id_documents is None:
            return None
        identifications = []
        for id_doc in id_documents:
            doc = {
                "Type": id_doc.document_type,
                "Value": id_doc.id_registration_number,
                "Country": id_doc.issuing_country,

            }
            if id_doc.dates:
                doc["Dates"] = id_doc.dates
            if id_doc.issued_in_location is not None:
                doc["Issued Location"] = AliasGoldUDFs.get_address(id_doc.issued_in_location)
            #if profile_id == 17283:
            #        print(f"profile_id :: {profile_id} ..... before conversion :: {id_doc} ..... doc :: {doc}")
            identifications.append(doc)
        #print(f"identifications :: {identifications}")
        return identifications


    @staticmethod
    @udf(returnType=ArrayType(MapType(StringType(), StringType())))
    def extract_addresses(features):
        if features is None:
            return None
        addresses = []
        for feature in features:
            if feature.type == "Location":
                for loc in feature.feature_versions[0].locations:
                    add = AliasGoldUDFs.get_address(loc)
                    addresses.append(add)
        return addresses

    @staticmethod
    def get_address(loc):
        add = {}
        # Process area
        if loc.area and len(loc.area) > 0:
            area = loc.area[0]
            if area.description != "Unknown" or area.value != "Unknown":
                add['Area'] = area.description if area.description != "Unknown" else area.value
        # Process country
        if loc.country and len(loc.country) > 0:
            country = loc.country[0]
            if country.country != "Unknown":
                add['Country'] = country.country
        # Process parts
        address_parts = []
        for part in loc.parts:
            if part.part_value and len(part.part_value) > 0:
                part_value = part.part_value[0].location_value
                if part_value and part_value != "Unknown":
                    if part.part_type.startswith("ADDRESS"):
                        address_parts.append(part_value)
                    else:
                        add[part.part_type.capitalize()] = part_value
        if address_parts:
            add['Address'] = ', '.join(address_parts)
        return add


