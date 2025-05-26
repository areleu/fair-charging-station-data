# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from annotate import get_clean_data, INPUT_METADATA_FILE
import pandas as pd
import yaml
import frictionless as fl
from collections import OrderedDict
from copy import deepcopy
import json
from os import mkdir, path
from zlib import crc32

COLUMN_DATA = "bnetza_charging_columns_{dd}_{mm}_{yyyy}"
FACILITY_DATA = "bnetza_facilities_{dd}_{mm}_{yyyy}"
POINT_DATA = "bnetza_charging_points_{dd}_{mm}_{yyyy}"
OPERATOR_DATA = "bnetza_operators_{dd}_{mm}_{yyyy}"
GEOLOCATION_DATA = "bnetza_locations_{dd}_{mm}_{yyyy}"
ADDRESS_DATA = "bnetza_addresses_{dd}_{mm}_{yyyy}"
COORDINATE_DATA = "bnetza_coordinates_{dd}_{mm}_{yyyy}"
NORMALIZED_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"
COMPATIBILITY_DATA = "bnetza_compatibility_{dd}_{mm}_{yyyy}"
SOCKET_DATA = "bnetza_charging_sockets_{dd}_{mm}_{yyyy}"
NORMALISEDIR = "normalised"

CONNECTION_TYPE_MAP = {
    "AC Typ 2 Steckdose": "ac_iec62196t2_socket",
    "AC Typ 2 Fahrzeugkupplung": "ac_iec62196t2_cable",
    "AC Typ 1 Steckdose": "ac_iec62196t1_socket",
    "AC Schuko": "ac_domesticf_socket",
    "DC CHAdeMO": "dc_chademo_socket",
    "DC Fahrzeugkupplung Typ Combo 2 (CCS)": "dc_iec62196t2combo_cable",
    "DC Tesla Fahrzeugkupplung (Typ 2)": "dc_tesla_cable",
}


def get_normalised_data(download_date: tuple = None):
    df, filename, (dd, mm, yyyy) = get_clean_data(download_date)

    df = df.set_index("Ladeeinrichtungs-ID")
    df.index.name = "id"

    column_data = df.iloc[:, :22]

    # socket data
    ci = "column_id"
    oi = "operator_id"
    gi = "geolocation_id"
    pi = "point_id"
    si = "socket_id"
    fi = "facility_id"

    column_names = ["Steckertypen", "Leistungskapazität", "EVSE-ID", "Key"]
    point_data_1 = df.iloc[:, 22:26]
    point_data_1.columns = [column_names]
    point_data_2 = df.iloc[:, 26:30]
    point_data_2.columns = [column_names]
    point_data_3 = df.iloc[:, 30:34]
    point_data_3.columns = [column_names]
    point_data_4 = df.iloc[:, 34:38]
    point_data_4.columns = [column_names]
    point_data_5 = df.iloc[:, 38:42]
    point_data_5.columns = [column_names]
    point_data_6 = df.iloc[:, 42:46]
    point_data_6.columns = [column_names]

    point_data = pd.concat(
        [
            point_data_1.reset_index(),
            point_data_2.reset_index(),
            point_data_3.reset_index(),
            point_data_4.reset_index(),
            point_data_5.reset_index(),
            point_data_6.reset_index(),
        ],
        ignore_index=True,
    ).rename(columns={"id": "column_id"})

    point_data.columns = [c[0] for c in point_data.columns]
    point_data.dropna(
        subset=column_names,
        how="all",
        inplace=True,
    )
    point_data.sort_values("column_id", inplace=True)
    point_data.reset_index(inplace=True)
    point_data.drop(columns="index", inplace=True)
    point_data.index.name = "id"

    point_filename = POINT_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    column_filename = COLUMN_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    facility_filename = FACILITY_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    operator_filename = OPERATOR_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    geolocation_filename = GEOLOCATION_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    address_filename = ADDRESS_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    coordinate_filename = COORDINATE_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    socket_filename = SOCKET_DATA.format(dd=dd, mm=mm, yyyy=yyyy)
    compatibility_filename = COMPATIBILITY_DATA.format(dd=dd, mm=mm, yyyy=yyyy)

    point_data["types_temp"] = (
        point_data["Steckertypen"]
        .str.replace(";", ",")
        .fillna("")
        .str.split(",")
        .apply(
            lambda lst: [
                CONNECTION_TYPE_MAP.get(itm.strip(), itm.strip()) for itm in lst
            ]
        )
    )
    point_data["power_temp"] = (
        point_data["Leistungskapazität"]
        .astype(str)
        .str.split(";")
        .map(lambda x: [y.replace(",", ".").strip() for y in x])
    )
    point_data["sockets_temp"] = point_data.apply(
        lambda row: ",".join(
            ["_".join(s) for s in zip(row["types_temp"], row["power_temp"])]
        ),
        axis=1,
    )
    socket_types = [
        it
        for it in set(
            item.strip()
            for sublist in list(
                str(u).replace(";", ",").split(",")
                for u in point_data["sockets_temp"].unique()
            )
            for item in sublist
        )
        if len(it) > 0
    ]
    socket_data = pd.DataFrame({"name": socket_types})
    socket_data[["current", "pattern", "connector", "power"]] = socket_data[
        "name"
    ].str.split("_", expand=True)
    socket_data = socket_data.applymap(lambda v: v if v != "None" else None)
    socket_data.index.name = "id"

    compatibility_base = pd.DataFrame(
        [
            (tup.id, t)
            for tup in point_data.reset_index().itertuples()
            for t in tup.sockets_temp.split(",")
            if len(t) > 0
        ]
    )
    compatibility_base.columns = ["point_id", "socket_type"]
    compatibility_data = (
        pd.merge(
            compatibility_base,
            socket_data.reset_index()[["id", "name"]],
            left_on="socket_type",
            right_on="name",
            how="left",
        )
        .rename(columns={"id": "socket_id"})
        .drop(columns=["socket_type", "name"])
    )
    compatibility_data.index.name = "id"

    point_data.drop(
        columns=[
            "types_temp",
            "power_temp",
            "sockets_temp",
            "Steckertypen",
            "Leistungskapazität",
        ],
        inplace=True,
    )
    socket_data.drop(columns=["name"], inplace=True)
    # Separate operators
    column_data["Betreiber"] = column_data["Betreiber"].str.strip()
    operator_data = pd.DataFrame({"Betreiber": column_data["Betreiber"].unique()})
    operator_data.sort_values(by="Betreiber")
    operator_data.index.name = "id"
    new_columns = pd.merge(
        column_data.reset_index(),
        operator_data.reset_index()[["Betreiber", "id"]],
        left_on="Betreiber",
        right_on="Betreiber",
        how="left",
        sort=False,
    ).set_index("id_x")
    new_columns.index.name = "id"
    column_data.insert(loc=1, column=oi, value=new_columns["id_y"])
    column_data.drop(columns=["Betreiber"], inplace=True)

    # Separate locations
    location_columns = [
        "Straße",
        "Hausnummer",
        "Adresszusatz",
        "Ort",
        "Bundesland",
        "Kreis/kreisfreie Stadt",
        "Standortbezeichnung",
        "Postleitzahl",
    ]
    numeric_location_columns = ["Breitengrad", "Längengrad"]
    all_locations = location_columns + numeric_location_columns

    column_data["Postleitzahl"] = column_data["Postleitzahl"].astype(str)
    column_data[location_columns] = column_data[location_columns].apply(
        lambda x: x.str.strip()
    )
    column_data["identifier"] = column_data[all_locations].astype(str).sum(axis=1)

    geolocation_data = column_data[all_locations + ["identifier"]]
    geolocation_data.drop_duplicates(inplace=True)
    geolocation_data = geolocation_data.reset_index(drop=True)
    geolocation_data.index.name = "id"
    column_data.drop(columns=all_locations, inplace=True)
    new_columns = pd.merge(
        column_data.reset_index(),
        geolocation_data.reset_index()[["identifier", "id"]],
        left_on="identifier",
        right_on="identifier",
        how="left",
        sort=False,
    ).set_index("id_x")
    new_columns.index.name = "id"
    column_data.insert(loc=1, column=gi, value=new_columns["id_y"])
    column_data.drop(columns=["identifier"], inplace=True)

    opening_times_map = {
        "Keine Angabe": None,
        "247": "247",
        "Eingeschränkt": "Eingeschränkt",
    }
    column_data["Öffnungszeiten"] = column_data["Öffnungszeiten"].apply(
        lambda x: opening_times_map.get(x, x)
    )
    facility_columns = [
        "operator_id",
        "geolocation_id",
        "Öffnungszeiten",
        "Öffnungszeiten: Wochentage",
        "Öffnungszeiten: Tageszeiten",
    ]
    facility_data_pre = (
        column_data[facility_columns]
        .reset_index()
        .drop(columns=["id"])
        .drop_duplicates()
    )
    dup_filter = facility_data_pre.duplicated(
        subset=["operator_id", "geolocation_id"], keep=False
    )
    duplicated = (
        facility_data_pre[dup_filter]
        .groupby(["operator_id", "geolocation_id"])
        .agg("first")
    ).reset_index()
    uniques = facility_data_pre[~dup_filter]

    facility_data = pd.concat([uniques, duplicated])
    facility_data["id"] = facility_data.apply(
        lambda row: str(row["operator_id"]).zfill(6) + str(row["geolocation_id"]).zfill(6),
        axis=1,
    )
    column_data = column_data.join(
        facility_data[["operator_id", "geolocation_id", "id"]].set_index(
            ["operator_id", "geolocation_id"]
        ),
        on=["operator_id", "geolocation_id"],
        how="left",
    ).rename(columns={"id": "facility_id"})

    facility_data = facility_data.set_index("id")
    column_data = column_data.drop(columns=facility_columns)
    column_data = column_data[
        [column_data.columns[-1]] + list(column_data.columns[:-1])
    ]

    geolocation_data.drop(columns=["identifier"], inplace=True)

    # Annotate
    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # column data
    # get  file schema
    column_schema = fl.Schema.describe(column_data)
    column_dict = column_schema.to_dict()

    column_fields = OrderedDict({f["name"]: f for f in column_dict["fields"]})
    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in column_fields.keys()
    }
    annotation_fields[fi] = {"description": "Identifier of the facility"}

    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
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
                {
                    "fields": [fi],
                    "reference": {"resource": operator_filename, "fields": ["id"]},
                },
            ],
        },
    }

    # facility data
    # get  file schema
    facility_schema = fl.Schema.describe(facility_data)
    facility_dict = facility_schema.to_dict()

    facility_fields = OrderedDict({f["name"]: f for f in facility_dict["fields"]})
    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in facility_fields.keys()
    }
    annotation_fields[oi] = {"description": "Identifier of the operator"}
    annotation_fields[gi] = {"description": "Identifier of the location"}

    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
        facility_fields[k].update(v)
    if "id" in facility_fields:
        if "constraints" in facility_fields["id"]:
            facility_fields["id"].pop("constraints")
    facility_fields_list = [v for v in facility_fields.values()]
    facility_resource = {
        "profile": "tabular-data-resource",
        "name": facility_filename,
        "path": f"{facility_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": facility_fields_list,
            "primaryKey": ["id"],
            "foreignKeys": [
                {
                    "fields": [oi],
                    "reference": {"resource": operator_filename, "fields": ["id"]},
                },
                {
                    "fields": [gi],
                    "reference": {"resource": geolocation_filename, "fields": ["id"]},
                },
            ],
        },
    }

    # socket data
    # get file schema
    point_schema = fl.Schema.describe(point_data)
    point_dict = point_schema.to_dict()

    point_fields = OrderedDict({f["name"]: f for f in point_dict["fields"]})

    reference_fields = {
        "P1 [kW]": "Leistungskapazität",
        "Public Key1": "Key",
    }  # "Steckertypen1": "Steckertypen",
    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in reference_fields.keys()
    }
    annotation_fields["id"] = {"description": "Unique identifier"}
    annotation_fields["column_id"] = {"description": "Identifier of column"}
    for k, v in annotation_fields.items():
        point_fields[reference_fields.get(k, k)].update(v)
        point_fields[reference_fields.get(k, k)]["name"] = reference_fields.get(k, k)
        point_fields[reference_fields.get(k, k)]["description"] = point_fields[
            reference_fields.get(k, k)
        ]["description"].replace(" first", "")
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
                {
                    "fields": [ci],
                    "reference": {"resource": column_filename, "fields": ["id"]},
                }
            ],
        },
    }
    operator_schema = fl.Schema.describe(operator_data)
    operator_dict = operator_schema.to_dict()

    operator_fields = OrderedDict({f["name"]: f for f in operator_dict["fields"]})

    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in operator_fields.keys()
    }
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
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
        "schema": {"fields": operator_fields_list, "primaryKey": ["id"]},
    }

    location_schema = fl.Schema.describe(location_data)
    location_dict = location_schema.to_dict()

    location_fields = OrderedDict({f["name"]: f for f in location_dict["fields"]})

    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in location_fields.keys()
    }
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
        location_fields[k].update(v)
    if "id" in location_fields:
        if "constraints" in location_fields["id"]:
            location_fields["id"].pop("constraints")
    location_fields_list = [v for v in location_fields.values()]
    geolocation_resource = {
        "profile": "tabular-data-resource",
        "name": geolocation_filename,
        "path": f"{geolocation_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {"fields": location_fields_list, "primaryKey": ["id"]},
    }
    # socket data
    socket_schema = fl.Schema.describe(socket_data)
    socket_dict = socket_schema.to_dict()

    socket_fields = OrderedDict({f["name"]: f for f in socket_dict["fields"]})

    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in socket_fields.keys()
    }
    annotation_fields["id"] = {"description": "Unique identifier"}
    annotation_fields["current"] = {"description": "Current mode of the socket"}
    annotation_fields["pattern"] = {
        "description": "Connection pattern of the connector"
    }
    annotation_fields["connector"] = {"description": "Type of coupling"}
    annotation_fields["power"] = {
        "description": "Max power supported by the connector."
    }
    # annotation_fields["maker"] = {"description": "Developer of the socket type"}
    # annotation_fields["mode"] = {"description": "Charging mode of the connector"}
    for k, v in annotation_fields.items():
        socket_fields[k].update(v)
    if "id" in socket_fields:
        if "constraints" in socket_fields["id"]:
            socket_fields["id"].pop("constraints")
    socket_field_list = [v for v in socket_fields.values()]
    socket_resource = {
        "profile": "tabular-data-resource",
        "name": socket_filename,
        "path": f"{socket_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {"fields": socket_field_list, "primaryKey": ["id"]},
    }

    # compatibility data
    # get  file schema
    compatibility_schema = fl.Schema.describe(compatibility_data)
    compatibility_dict = compatibility_schema.to_dict()

    compatibility_fields = OrderedDict(
        {f["name"]: f for f in compatibility_dict["fields"]}
    )
    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in compatibility_fields.keys()
    }
    annotation_fields[pi] = {"description": "Identifier of the charging point"}
    annotation_fields[si] = {"description": "Identifier of the compatible sockets."}

    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
        compatibility_fields[k].update(v)
    if "id" in compatibility_fields:
        if "constraints" in compatibility_fields["id"]:
            compatibility_fields["id"].pop("constraints")
    compatibility_fields_list = [v for v in compatibility_fields.values()]
    compatibility_resource = {
        "profile": "tabular-data-resource",
        "name": compatibility_filename,
        "path": f"{compatibility_filename}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": compatibility_fields_list,
            "primaryKey": ["id"],
            "foreignKeys": [
                {
                    "fields": [pi],
                    "reference": {"resource": point_filename, "fields": ["id"]},
                },
                {
                    "fields": [si],
                    "reference": {"resource": socket_filename, "fields": ["id"]},
                },
            ],
        },
    }

    annotations_new = deepcopy(annotations)
    annotations_new["name"] = f"{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}"
    annotations_new["title"] = "FAIR Charging Station data (Normalised)"
    annotations_new["description"] = (
        "Normalised dataset based on the BNetzA charging station data."
    )
    annotations_new["publicationDate"] = f"{yyyy}-{mm}-{dd}"
    annotations_new["resources"] = [
        column_resource,
        facility_resource,
        point_resource,
        operator_resource,
        geolocation_resource,
        socket_resource,
        compatibility_resource,
    ]

    data = {
        "column": column_data,
        "facility": facility_data,
        "point": point_data,
        "operator": operator_data,
        "geolocation": geolocation_data,
        "socket": socket_data,
        "compatibility": compatibility_data,
    }
    filenames = {
        "column": column_filename,
        "facility": facility_filename,
        "point": point_filename,
        "operator": operator_filename,
        "geolocation": geolocation_filename,
        "socket": socket_filename,
        "compatibility": compatibility_filename,
    }
    return data, filenames, annotations_new, (dd, mm, yyyy)


def main():
    data, filenames, annotations_new, (dd, mm, yyyy) = get_normalised_data()
    # export

    if not path.exists(f"{NORMALISEDIR}"):
        mkdir(NORMALISEDIR)

    for element in data.keys():
        data[element].to_csv(
            f"{NORMALISEDIR}/{filenames[element]}.csv",
            date_format="%Y-%m-%d %H:%M:%S",
        )

    with open(
        f"{NORMALISEDIR}/{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json",
        "w",
        encoding="utf8",
    ) as output:
        json.dump(annotations_new, output, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
