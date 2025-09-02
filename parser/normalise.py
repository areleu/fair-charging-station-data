# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from .annotate import get_clean_data, INPUT_METADATA_FILE
import pandas as pd
import yaml
import frictionless as fl
from collections import OrderedDict
from copy import deepcopy
import json
from os import mkdir, path
from zlib import crc32
import numpy as np

COLUMN_DATA = "bnetza_charging_columns_{dd}_{mm}_{yyyy}"
FACILITY_DATA = "bnetza_facilities_{dd}_{mm}_{yyyy}"
POINT_DATA = "bnetza_charging_points_{dd}_{mm}_{yyyy}"
OPERATOR_DATA = "bnetza_operators_{dd}_{mm}_{yyyy}"
GEOLOCATION_DATA = "bnetza_geolocations_{dd}_{mm}_{yyyy}"
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


def describe_and_annotate(
    data, annotations, resource_name, primary_key, foreign_keys=None
):
    """
    Describe the schema of the data, annotate it, and return the resource dictionary.
    """
    schema = fl.Schema.describe(data)
    schema_dict = schema.to_dict()

    fields = OrderedDict({f["name"]: f for f in schema_dict["fields"]})
    annotation_fields = {
        f["name"]: f
        for f in annotations["resources"][0]["schema"]["fields"]
        if f["name"] in fields.keys()
    }

    # Add common annotations
    annotation_fields["id"] = {"description": "Unique identifier"}
    for k, v in annotation_fields.items():
        fields[k].update(v)

    # Remove constraints from "id" if present
    if "id" in fields and "constraints" in fields["id"]:
        fields["id"].pop("constraints")

    fields_list = [v for v in fields.values()]
    resource = {
        "profile": "tabular-data-resource",
        "name": resource_name,
        "path": f"{resource_name}.csv",
        "format": "csv",
        "encoding": "utf-8",
        "schema": {
            "fields": fields_list,
            "primaryKey": [primary_key],
        },
    }

    if foreign_keys:
        resource["schema"]["foreignKeys"] = foreign_keys

    return resource


def create_foreign_key(fields, reference_resource, reference_fields):
    """
    Create a foreign key dictionary.
    """
    return {
        "fields": fields,
        "reference": {"resource": reference_resource, "fields": reference_fields},
    }


def process_resources(data_dict, annotations, filenames, foreign_key_map):
    """
    Process all resources by describing, annotating, and creating resource dictionaries.
    """
    resources = []
    for key, data in data_dict.items():
        resource_name = filenames[key]
        primary_key = "id"
        foreign_keys = foreign_key_map.get(key, None)
        resource = describe_and_annotate(
            data, annotations, resource_name, primary_key, foreign_keys
        )
        resources.append(resource)
    return resources


def get_normalised_data(
    filename: str | None = None, download_date: tuple | None = None
):
    df, filename, (dd, mm, yyyy) = get_clean_data(filename, download_date)

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
    ai = "address_id"
    coi = "coordinate_id"

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
    address_columns = [
        "Straße",
        "Hausnummer",
        "Adresszusatz",
        "Ort",
        "Bundesland",
        "Kreis/kreisfreie Stadt",
        "Standortbezeichnung",
        "Postleitzahl",
    ]
    coordinate_columns = ["Breitengrad", "Längengrad"]
    all_locations = address_columns + coordinate_columns

    column_data["Postleitzahl"] = column_data["Postleitzahl"].astype(str)
    column_data["Hausnummer"] = column_data["Hausnummer"].astype(str)
    column_data[address_columns] = column_data[address_columns].apply(
        lambda x: x.str.strip()
    )

    column_data[ai] = column_data[address_columns].apply(
        lambda x: str(
            crc32(
                "".join(
                    [str(y).replace("nan", "").replace("None", "") for y in x]
                ).encode("utf8")
            )
        ),
        axis=1,
    )
    column_data[coi] = column_data[coordinate_columns].apply(
        lambda x: str(int(x["Breitengrad"] * 100000))
        + str(int(x["Längengrad"] * 100000)).replace("-", "1"),
        axis=1,
    )
    column_data["geolocation_id"] = column_data.apply(
        lambda x: "A" + str(x[ai]).zfill(6)[0:6] + "S" + str(x[coi]),
        axis=1,
    )

    address_data = column_data[address_columns + [ai]]
    address_data = (
        address_data.drop_duplicates().rename(columns={ai: "id"}).set_index("id")
    )

    coordinate_data = column_data[coordinate_columns + [coi]]
    coordinate_data = (
        coordinate_data.drop_duplicates().rename(columns={coi: "id"}).set_index("id")
    )

    opening_times_map = {
        "Keine Angabe": None,
        "247": "247",
        "Eingeschränkt": "Eingeschränkt",
    }
    column_data["Öffnungszeiten"] = column_data["Öffnungszeiten"].apply(
        lambda x: opening_times_map.get(x, x)
    )
    # If the coordinate and the address pair points to a single row, it means that the coordinate points to the column!
    column_data["coord_points_column"] = ~column_data.duplicated(subset=[ai, coi])
    # Address has multiple columns
    column_data["facility_has_multiple_columns"] = column_data.duplicated(subset=[ai])
    # If the coordinate and the address pair points to multiple columns, it means that the coordinate points to the facility!
    # If the pair is unique and the facility has one column, the coordinate also points to the facility
    column_data["coord_points_facility"] = (
        column_data.duplicated(subset=[ai, coi])
        | ~column_data["facility_has_multiple_columns"]
    )

    facility_columns = [
        "operator_id",
        coi,
        ai,
        "Öffnungszeiten",
        "Öffnungszeiten: Wochentage",
        "Öffnungszeiten: Tageszeiten",
        "coord_points_facility",
    ]
    facility_data_pre = (
        column_data[facility_columns]
        .reset_index()
        .drop(columns=["id"])
        .drop_duplicates()
    )
    dup_filter = facility_data_pre.duplicated(subset=["operator_id", ai], keep=False)
    duplicated = (
        facility_data_pre[dup_filter].groupby(["operator_id", ai]).agg("first")
    ).reset_index()
    uniques = facility_data_pre[~dup_filter]

    facility_data = pd.concat([uniques, duplicated])
    facility_data["id"] = facility_data.apply(
        lambda row: str(row["operator_id"]).zfill(6) + str(row[ai]).zfill(6)[:6],
        axis=1,
    )
    facility_data[coi] = facility_data.apply(
        lambda row: row[coi] if row["coord_points_facility"] else None, axis=1
    )
    column_data = column_data.join(
        facility_data[["operator_id", ai, "id"]].set_index(["operator_id", ai]),
        on=["operator_id", ai],
        how="left",
    ).rename(columns={"id": "facility_id"})

    column_data[coi] = column_data.apply(
        lambda row: row[coi] if row["coord_points_column"] else None, axis=1
    )
    column_data["geolocation_id"] = column_data.apply(
        lambda x: "A"
        + str(x[ai]).zfill(6)[0:6]
        + "S"
        + str(x[coi]).replace("None", "").zfill(14),
        axis=1,
    )

    facility_data["geolocation_id"] = facility_data.apply(
        lambda x: "A"
        + str(x[ai]).zfill(6)[0:6]
        + "S"
        + str(x[coi]).replace("None", "").zfill(14),
        axis=1,
    )

    facility_data = facility_data.set_index("id")

    geolocation_data = pd.concat(
        [
            column_data[["geolocation_id", ai, coi]],
            facility_data[["geolocation_id", ai, coi]],
        ]
    )
    geolocation_data = (
        geolocation_data.drop_duplicates()
        .rename(columns={"geolocation_id": "id"})
        .set_index("id")
    )
    column_data = column_data.drop(
        columns=facility_columns
        + all_locations
        + ["coord_points_column", "facility_has_multiple_columns"]
    )
    facility_data = facility_data.drop(columns=[coi, ai, "coord_points_facility"])
    facility_data = facility_data[
        [facility_data.columns[-1]] + list(facility_data.columns[:-1])
    ]
    column_data = column_data[
        list(column_data.columns[-2:]) + list(column_data.columns[:-2])
    ]

    # Annotate
    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # Define data and filenames
    data_dict = {
        "column": column_data,
        "facility": facility_data,
        "point": point_data,
        "operator": operator_data,
        "geolocation": geolocation_data,
        "socket": socket_data,
        "compatibility": compatibility_data,
        "address": address_data,
        "coordinate": coordinate_data,
    }
    filenames = {
        "column": column_filename,
        "facility": facility_filename,
        "point": point_filename,
        "operator": operator_filename,
        "geolocation": geolocation_filename,
        "socket": socket_filename,
        "compatibility": compatibility_filename,
        "address": address_filename,
        "coordinate": coordinate_filename,
    }

    # Define foreign key mappings
    foreign_key_map = {
        "column": [
            create_foreign_key([fi], operator_filename, ["id"]),
            create_foreign_key([gi], geolocation_filename, ["id"]),
        ],
        "facility": [
            create_foreign_key([oi], operator_filename, ["id"]),
            create_foreign_key([gi], geolocation_filename, ["id"]),
        ],
        "point": [
            create_foreign_key([ci], column_filename, ["id"]),
        ],
        "geolocation": [
            create_foreign_key([ai], address_filename, ["id"]),
            create_foreign_key([coi], coordinate_filename, ["id"]),
        ],
        "compatibility": [
            create_foreign_key([pi], point_filename, ["id"]),
            create_foreign_key([si], socket_filename, ["id"]),
        ],
    }

    # Process resources
    resources = process_resources(data_dict, annotations, filenames, foreign_key_map)

    # Update annotations
    annotations_new = deepcopy(annotations)
    annotations_new["name"] = f"{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}"
    annotations_new["title"] = "FAIR Charging Station data (Normalised)"
    annotations_new["description"] = (
        "Normalised dataset based on the BNetzA charging station data."
    )
    annotations_new["publicationDate"] = f"{yyyy}-{mm}-{dd}"
    annotations_new["resources"] = resources

    return data_dict, filenames, annotations_new, (dd, mm, yyyy)


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
