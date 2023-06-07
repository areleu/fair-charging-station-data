from annotate import get_clean_data, FAIRDIR, INPUT_METADATA_FILE
import pandas as pd
import yaml
import frictionless as fl
from collections import OrderedDict
from copy import deepcopy
from omi.dialects.oep.dialect import OEP_V_1_5_Dialect
import json 

COLUMN_DATA = "bnetza_charging_columns_{dd}_{mm}_{yyyy}"
SOCKET_DATA = "bnetza_charging_sockets_{dd}_{mm}_{yyyy}"
NORMALIZED_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"

def main():
    df, filename, (dd, mm, yyyy) = get_clean_data()

    df.index.name = "id"

    column_data = df.iloc[:,:14]

    # socket data
    ci = "column_id"
    column_names = [ci , "Steckertypen", "Leistungskapazität", "PublicKey"]
    socket_data_1 = df.iloc[:,14:17].reset_index().rename(columns={"id": ci})
    socket_data_1.columns = [column_names]
    socket_data_2 = df.iloc[:,17:20].reset_index().rename(columns={"id": ci})
    socket_data_2.columns = [column_names]
    socket_data_3 = df.iloc[:,20:23].reset_index().rename(columns={"id": ci})
    socket_data_3.columns = [column_names]
    socket_data_4 = df.iloc[:,23:26].reset_index().rename(columns={"id": ci})
    socket_data_4.columns = [column_names]

    socket_data = pd.concat([socket_data_1, socket_data_2, socket_data_3, socket_data_4], ignore_index=True)

    socket_data.columns = [c[0] for c in socket_data.columns]
    socket_data.dropna(subset=["Steckertypen", "Leistungskapazität", "PublicKey"], how='all', inplace=True)
    socket_data.sort_values("column_id", inplace=True)
    socket_data.reset_index(inplace=True)
    socket_data.drop(columns="index", inplace=True)
    socket_data.index.name = "id"

    # export 
    column_filename = COLUMN_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    column_data.to_csv(f"{FAIRDIR}/{column_filename}.csv")
    socket_filename = SOCKET_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    socket_data.to_csv(f"{FAIRDIR}/{socket_filename}.csv")

    # Annotate
    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # column data
    # get  file schema
    column_schema = fl.Schema.describe(f"{FAIRDIR}/{column_filename}.csv", encoding="utf-8")
    column_dict = column_schema.to_dict()

    column_fields = OrderedDict(
        {f["name"]: f for f in column_dict["fields"]}
    )
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in column_fields.keys()}

    for k,v in annotation_fields.items():
        column_fields[k].update(v)
    column_fields_list = [v for v in column_fields.values()]
    column_resource = {
        "profile": "tabular-data-resource",
        "name": column_filename,
        "path": column_filename,
        "format": "CSV",
        "encoding": "UTF-8",
        "schema": {
            "fields": column_fields_list,
            "primaryKey": ["id"]
        }
    }

    # socket data
    # get file schema
    socket_schema = fl.Schema.describe(f"{FAIRDIR}/{socket_filename}.csv", encoding="utf-8")
    socket_dict = socket_schema.to_dict()

    socket_fields = OrderedDict(
        {f["name"]: f for f in socket_dict["fields"]}
    )

    reference_fields = {"Steckertypen1": "Steckertypen", "P1 [kW]":"Leistungskapazität", "Public Key1":"PublicKey"}
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in reference_fields.keys()}

    for k,v in annotation_fields.items():
        socket_fields[reference_fields[k]].update(v)
    socket_fields_list = [v for v in socket_fields.values()]
    socket_resource = {
        "profile": "tabular-data-resource",
        "name": socket_filename,
        "path": socket_filename,
        "format": "CSV",
        "encoding": "UTF-8",
        "schema": {
            "fields": socket_fields_list,
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
    annotations_new = deepcopy(annotations)
    annotations_new["name"] = f"{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}"
    annotations_new["title"] = "FAIR Charging Station data (Normalised)"
    annotations_new["description"] = "Normalised dataset based on the BNetzA charging station data."
    annotations_new["publicationDate"] = f"{yyyy}-{mm}-{dd}"
    annotations_new["resources"] = [column_resource, socket_resource]
    # annotations_new["sources"] = [
    #     {"title" : annotations["title"],
    #      "description": annotations["description"],
    #      "path": annotations["id"],
    #      "licenses": annotations["licenses"]}
    # ]

    dialect1_5 = OEP_V_1_5_Dialect()
    compiled = dialect1_5.compile(annotations_new)

    with open(f"{FAIRDIR}/{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(compiled, output, indent=4, ensure_ascii=False)
        
if __name__=="__main__":
    main()