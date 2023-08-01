# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from annotate import get_clean_data, INPUT_METADATA_FILE
import pandas as pd
import yaml
import frictionless as fl
from collections import OrderedDict
from copy import deepcopy
from omi.dialects.oep.dialect import OEP_V_1_5_Dialect
import json
from os import mkdir, path

COLUMN_DATA = "bnetza_charging_columns_{dd}_{mm}_{yyyy}"
SOCKET_DATA = "bnetza_charging_points_{dd}_{mm}_{yyyy}"
OPERATOR_DATA = "bnetza_operators_{dd}_{mm}_{yyyy}"
LOCATION_DATA = "bnetza_locations_{dd}_{mm}_{yyyy}"
NORMALIZED_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"
NORMALISEDIR = "normalised"

CONNECTION_TYPE_MAP = {
    "DC Kupplung Tesla Typ 2": "DC_type2_cablefemale_tesla",
    "AC CEE 3 polig": "AC_CEE3polig",
    "AC CEE 5 polig": "AC_CEE5polig",
    "CEE-Stecker": "AC_CEE_socketmale",
    "AC / CEE": "AC_CEE",
    "DC Kupplung Combo": "DC_CCS_cablefemale",
    "Adapter Typ1 \xa0Auto auf Typ2 Fahrzeugkupplung": "AC_type1",
    "AC Kupplung Typ 2": "AC_type2_cablefemale",
    "AC Steckdose Typ 2": "AC_type2_socketmale",
    "Typ 2 / Tesla": "DC_type2_cablefemale_tesla",
    "Tesla": "DC_type2_cablefemale_tesla",
    "AC Schuko": "AC_Schuko",
    "DC CHAdeMO": "DC_CHadeMO"

}

def get_normalised_data(download_date: tuple = None):
    df, filename, (dd, mm, yyyy) = get_clean_data(download_date)

    df.index.name = "id"

    column_data = df.iloc[:,:14]

    # socket data
    ci = "column_id"
    oi = "operator_id"
    li = "location_id"
    column_names = [ci , "Steckertypen", "Leistungskapazität", "PublicKey"]
    point_data_1 = df.iloc[:,14:17].reset_index().rename(columns={"id": ci})
    point_data_1.columns = [column_names]
    point_data_2 = df.iloc[:,17:20].reset_index().rename(columns={"id": ci})
    point_data_2.columns = [column_names]
    point_data_3 = df.iloc[:,20:23].reset_index().rename(columns={"id": ci})
    point_data_3.columns = [column_names]
    point_data_4 = df.iloc[:,23:26].reset_index().rename(columns={"id": ci})
    point_data_4.columns = [column_names]

    point_data = pd.concat([point_data_1, point_data_2, point_data_3, point_data_4], ignore_index=True)

    point_data.columns = [c[0] for c in point_data.columns]
    point_data.dropna(subset=["Steckertypen", "Leistungskapazität", "PublicKey"], how='all', inplace=True)
    point_data.sort_values("column_id", inplace=True)
    point_data.reset_index(inplace=True)
    point_data.drop(columns="index", inplace=True)
    point_data.index.name = "id"

    point_filename = SOCKET_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    column_filename = COLUMN_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    operator_filename = OPERATOR_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    location_filename = LOCATION_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    # point_data["types_temp"] = point_data["Steckertypen"].str.replace(";", ",").fillna("").str.split(",").apply(lambda lst: ",".join([CONNECTION_TYPE_MAP.get(itm.strip(), itm.strip()) for itm in lst]))
    # socket_types = set(item.strip() for sublist in list(str(u).replace(";", ",").split(",") for u in point_data["types_temp"].unique()) for item in sublist)

    # Separate operators
    column_data["Betreiber"] = column_data["Betreiber"].str.strip()
    operator_data = pd.DataFrame({"Betreiber": column_data["Betreiber"].unique()})
    operator_data.sort_values(by="Betreiber")
    operator_data.index.name = "id"
    new_columns = pd.merge(column_data.reset_index(), operator_data.reset_index()[["Betreiber", "id"]],
                           left_on="Betreiber", right_on="Betreiber", how="left", sort=False).set_index("id_x")
    new_columns.index.name = "id"
    column_data.insert(loc=1, column= oi, value= new_columns["id_y"])
    column_data.drop(columns=["Betreiber"], inplace=True)

    # Separate locations
    location_columns = ['Straße',
                        'Hausnummer',
                        'Adresszusatz',
                        'Ort',
                        'Bundesland',
                        'Kreis/kreisfreie Stadt']
    numeric_location_columns = ['Postleitzahl','Breitengrad', 'Längengrad']
    all_locations = location_columns + numeric_location_columns

    column_data[location_columns] = column_data[location_columns].apply(lambda x: x.str.strip())
    column_data["identifier"] = column_data[all_locations].astype(str).sum(axis=1)



    location_data = column_data[all_locations + ["identifier"]]
    location_data.drop_duplicates(inplace=True)
    location_data = location_data.reset_index(drop=True)
    location_data.index.name = "id"
    column_data.drop(columns=all_locations, inplace=True)
    new_columns = pd.merge(column_data.reset_index(), location_data.reset_index()[["identifier", "id"]],
                           left_on="identifier", right_on="identifier", how="left", sort=False).set_index("id_x")
    new_columns.index.name = "id"
    column_data.insert(loc=1, column= li, value= new_columns["id_y"])
    column_data.drop(columns=["identifier"], inplace=True)
    location_data.drop(columns=["identifier"], inplace=True)

    # Annotate
    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # column data
    # get  file schema
    column_schema = fl.Schema.describe(column_data)
    column_dict = column_schema.to_dict()

    column_fields = OrderedDict(
        {f["name"]: f for f in column_dict["fields"]}
    )
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in column_fields.keys()}
    annotation_fields[oi] = {"description": "Identifier of the operator"}
    annotation_fields[li] = {"description": "Identifier of the location"}

    annotation_fields["id"] = {"description": "Unique identifier"}
    for k,v in annotation_fields.items():
        column_fields[k].update(v)
    if "id" in column_fields:
        if "constraints" in column_fields["id"]:
            column_fields["id"].pop("constraints")
    column_fields_list = [v for v in column_fields.values()]
    column_resource = {
        "profile": "tabular-data-resource",
        "name": column_filename,
        "path": f"{column_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": column_fields_list,
            "primaryKey": ["id"],
            "foreignKeys": [
                {"fields": [oi],
                  "reference": {
                      "resource": operator_filename,
                      "fields": ["id"]
                  }},
                {"fields": [li],
                  "reference": {
                      "resource": location_filename,
                      "fields": ["id"]
                  }}
            ]
        }
    }

    # socket data
    # get file schema
    point_schema = fl.Schema.describe(point_data)
    point_dict = point_schema.to_dict()

    point_fields = OrderedDict(
        {f["name"]: f for f in point_dict["fields"]}
    )

    reference_fields = {"Steckertypen1": "Steckertypen", "P1 [kW]":"Leistungskapazität", "Public Key1":"PublicKey"}
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in reference_fields.keys()}
    annotation_fields["id"] = {"description": "Unique identifier"}
    annotation_fields["column_id"] = {"description": "Identifier of column"}
    for k,v in annotation_fields.items():
        point_fields[reference_fields.get(k,k)].update(v)
        point_fields[reference_fields.get(k,k)]["name"] = reference_fields.get(k,k)
        point_fields[reference_fields.get(k,k)]["description"] = point_fields[reference_fields.get(k,k)]["description"].replace(" first", "")
    if "id" in point_fields:
        if "constraints" in point_fields["id"]:
            point_fields["id"].pop("constraints")

    point_fields_list = [v for v in point_fields.values()]
    point_resource = {
        "profile": "tabular-data-resource",
        "name": point_filename,
        "path": f"{point_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": point_fields_list,
            "primaryKey": ["id"],
            "foreignKeys": [
                {"fields": [ci],
                  "reference": {
                      "resource": column_filename,
                      "fields": ["id"]
                  }}
            ]
        }
    }
    operator_schema = fl.Schema.describe(operator_data)
    operator_dict = operator_schema.to_dict()

    operator_fields = OrderedDict(
        {f["name"]: f for f in operator_dict["fields"]}
    )

    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in operator_fields.keys()}
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k,v in annotation_fields.items():
        operator_fields[k].update(v)
    if "id" in operator_fields:
        if "constraints" in operator_fields["id"]:
            operator_fields["id"].pop("constraints")
    operator_fields_list = [v for v in operator_fields.values()]
    operator_resource = {
        "profile": "tabular-data-resource",
        "name": operator_filename,
        "path": f"{operator_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": operator_fields_list,
            "primaryKey": ["id"]
        }
    }

    location_schema = fl.Schema.describe(location_data)
    location_dict = location_schema.to_dict()

    location_fields = OrderedDict(
        {f["name"]: f for f in location_dict["fields"]}
    )

    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in location_fields.keys()}
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k,v in annotation_fields.items():
        location_fields[k].update(v)
    if "id" in location_fields:
        if "constraints" in location_fields["id"]:
            location_fields["id"].pop("constraints")
    location_fields_list = [v for v in location_fields.values()]
    location_resource = {
        "profile": "tabular-data-resource",
        "name": location_filename,
        "path": f"{location_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": location_fields_list,
            "primaryKey": ["id"]
        }
    }

    annotations_new = deepcopy(annotations)
    annotations_new["name"] = f"{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}"
    annotations_new["title"] = "FAIR Charging Station data (Normalised)"
    annotations_new["description"] = "Normalised dataset based on the BNetzA charging station data."
    annotations_new["publicationDate"] = f"{yyyy}-{mm}-{dd}"
    annotations_new["resources"] = [column_resource, point_resource, operator_resource, location_resource]

    dialect1_5 = OEP_V_1_5_Dialect()
    compiled_metadata = dialect1_5.compile(annotations_new)

    return column_data, point_data, operator_data, location_data, column_filename, point_filename, operator_filename, location_filename, compiled_metadata, (dd, mm, yyyy)

def main():

    column_data, point_data, operator_data, location_data, column_filename, point_filename, operator_filename, location_filename, compiled_metadata, (dd, mm, yyyy) = get_normalised_data()
    # export

    if not path.exists(f"{NORMALISEDIR}"):
        mkdir(NORMALISEDIR)

    column_data.to_csv(f"{NORMALISEDIR}/{column_filename}.csv")
    point_data.to_csv(f"{NORMALISEDIR}/{point_filename}.csv")
    operator_data.to_csv(f"{NORMALISEDIR}/{operator_filename}.csv")
    location_data.to_csv(f"{NORMALISEDIR}/{location_filename}.csv")

    with open(f"{NORMALISEDIR}/{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(compiled_metadata, output, indent=4, ensure_ascii=False)

if __name__=="__main__":
    main()