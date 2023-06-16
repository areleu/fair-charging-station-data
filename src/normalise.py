"""BSD 3-Clause License

Copyright (c) 2023, German Aerospace Center (DLR)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

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
SOCKET_DATA = "bnetza_charging_sockets_{dd}_{mm}_{yyyy}"
NORMALIZED_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"
NORMALISEDIR = "normalised"

def get_normalised_data(download_date: tuple = None):
    df, filename, (dd, mm, yyyy) = get_clean_data(download_date)

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

    socket_filename = SOCKET_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    column_filename = COLUMN_DATA.format(dd=dd, mm=mm, yyyy=yyyy)

    # Annotate
    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # column data
    # get  file schema
    column_schema = fl.Schema.describe(f"{NORMALISEDIR}/{column_filename}.csv", encoding="utf-8")
    column_dict = column_schema.to_dict()

    column_fields = OrderedDict(
        {f["name"]: f for f in column_dict["fields"]}
    )
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in column_fields.keys()}
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k,v in annotation_fields.items():
        column_fields[k].update(v)
    column_fields_list = [v for v in column_fields.values()]
    column_resource = {
        "profile": "tabular-data-resource",
        "name": column_filename,
        "path": column_filename,
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": column_fields_list,
            "primaryKey": ["id"]
        }
    }

    # socket data
    # get file schema
    socket_schema = fl.Schema.describe(f"{NORMALISEDIR}/{socket_filename}.csv", encoding="utf-8")
    socket_dict = socket_schema.to_dict()

    socket_fields = OrderedDict(
        {f["name"]: f for f in socket_dict["fields"]}
    )

    reference_fields = {"Steckertypen1": "Steckertypen", "P1 [kW]":"Leistungskapazität", "Public Key1":"PublicKey"}
    annotation_fields = {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"] if f["name"] in reference_fields.keys()}
    annotation_fields["id"] = {"description": "Unique identifier"}
    annotation_fields["column_id"] = {"description": "Identifier of column"}
    for k,v in annotation_fields.items():
        socket_fields[reference_fields.get(k,k)].update(v)
        socket_fields[reference_fields.get(k,k)]["name"] = reference_fields.get(k,k)
        socket_fields[reference_fields.get(k,k)]["description"] = socket_fields[reference_fields.get(k,k)]["description"].replace(" first", "")
    socket_fields_list = [v for v in socket_fields.values()]
    socket_resource = {
        "profile": "tabular-data-resource",
        "name": socket_filename,
        "path": socket_filename,
        "format": "csv",
        "encoding": "utf-8",
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

    dialect1_5 = OEP_V_1_5_Dialect()
    compiled_metadata = dialect1_5.compile(annotations_new)

    return column_data, socket_data, column_filename, socket_filename, compiled_metadata, (dd, mm, yyyy)

def main():

    column_data, socket_data, column_filename, socket_filename, compiled_metadata, (dd, mm, yyyy) = get_normalised_data()
    # export

    if not path.exists(f"{NORMALISEDIR}"):
        mkdir(NORMALISEDIR)

    column_data.to_csv(f"{NORMALISEDIR}/{column_filename}.csv")
    socket_data.to_csv(f"{NORMALISEDIR}/{socket_filename}.csv")

    with open(f"{NORMALISEDIR}/{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(compiled_metadata, output, indent=4, ensure_ascii=False)

if __name__=="__main__":
    main()