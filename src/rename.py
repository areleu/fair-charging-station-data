# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path
from normalise import get_normalised_data
from annotate import annotate
import json

DEBUG = False
OEP = False  # The OEP format is not entirely compatible with frictionless, change to False to generate a frictionless dataset.

DEFAULT_DIR = "default"
OPERATIONAL_BASE = "operational"
OEP_NORMAL_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"
OEP_REGULAR_FILEANAME = "bnetza_charging_stations_{dd}_{mm}_{yyyy}"

COLUMN_RENAME = {
    "Betreiber": "operator",
    "Status": "status",
    "Anzeigename (Karte)": "display_name",
    "Straße": "thoroughfare",
    "Hausnummer": "locator_designator",
    "Adresszusatz": "address_supplement",
    "Standortbezeichnung": "location_description",
    "Postleitzahl": "postcode",
    "Ort": "post_name",
    "Bundesland": "federal_state",
    "Kreis/kreisfreie Stadt": "county",
    "Breitengrad": "latitude",
    "Längengrad": "longitude",
    "Inbetriebnahmedatum": "commissioning_date",
    "Nennleistung Ladeeinrichtung [kW]": "net_capacity",
    "Art der Ladeeinrichtung": "column_type",
    "Informationen zum Parkraum": "parking_information",
    "Bezahlsysteme": "paying_system",
    "Öffnungszeiten": "opening_times",
    "Öffnungszeiten: Wochentage": "opening_days",
    "Öffnungszeiten: Tageszeiten": "opening_hours",
    "Anzahl Ladepunkte": "charger_amount",
    "Steckertypen1": "charger_type_1",
    "Nennleistung Stecker1": "charger_power_1",
    "Public Key1": "charger_public_key_1",
    "EVSE-ID1": "evse_id_1",
    "Steckertypen2": "charger_type_2",
    "Nennleistung Stecker2": "charger_power_2",
    "Public Key2": "charger_public_key_2",
    "EVSE-ID2": "evse_id_2",
    "Steckertypen3": "charger_type_3",
    "Nennleistung Stecker3": "charger_power_3",
    "Public Key3": "charger_public_key_3",
    "EVSE-ID3": "evse_id_3",
    "Steckertypen4": "charger_type_4",
    "Nennleistung Stecker4": "charger_power_4",
    "Public Key4": "charger_public_key_4",
    "EVSE-ID4": "evse_id_4",
    "Steckertypen5": "charger_type_5",
    "Nennleistung Stecker5": "charger_power_5",
    "Public Key5": "charger_public_key_5",
    "EVSE-ID5": "evse_id_5",
    "Steckertypen6": "charger_type_6",
    "Nennleistung Stecker6": "charger_power_6",
    "Public Key6": "charger_public_key_6",
    "EVSE-ID6": "evse_id_6",
    "Steckertypen": "charger_type",
    "Leistungskapazität": "charger_power",
    "PublicKey": "public_key",
}

CONTENT_RENAME_TYPE = {
    "Normalladeeinrichtung": "regular",
    "Schnellladeeinrichtung": "fast",
}

COLUMN_DATATYPE = {
    "Postleitzahl": "integer",
    "Breitengrad": "float",
    "Längengrad": "float",
    "Nennleistung Ladeeinrichtung [kW]": "float",
    "Anzahl Ladepunkte": "integer",
    "Nennleistung Stecker1": "float",
    "Nennleistung Stecker2": "float",
    "Nennleistung Stecker3": "float",
    "Nennleistung Stecker4": "float",
    "Leistungskapazität": "float",
    "id": "integer",
    "column_id": "integer",
    "Inbetriebnahmedatum": "date",
}


def get_renamed_normalised(download_date: tuple = None, oep=True):
    # column_data, point_data, operator_data, location_data, _, _, _, _,
    data, filenames, normalised_compiled_metadata, (dd, mm, yyyy) = get_normalised_data(
        download_date
    )

    column_data = data["column"].rename(columns=COLUMN_RENAME)
    point_data = data["point"].rename(columns=COLUMN_RENAME)
    operator_data = data["operator"].rename(columns=COLUMN_RENAME)
    location_data = data["location"].rename(columns=COLUMN_RENAME)
    socket_data = data["socket"].rename(columns=COLUMN_RENAME)
    compatibility_data = data["compatibility"].rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        column_data["column_type"] = column_data["column_type"].str.replace(k, v)

    # Column resource renaming
    column_resource_fields = normalised_compiled_metadata["resources"][0]["schema"][
        "fields"
    ]
    new_fields_column = []
    for field in column_resource_fields:
        old_name = field["name"]
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        if "valueReference" in field.keys():
            new_refs = []
            for ref in field["valueReference"]:
                nr = ref
                nr["value"] = CONTENT_RENAME_TYPE[nr["value"]]
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields_column.append(field)
    assert set(f["name"] for f in new_fields_column) == set(
        list(column_data.columns) + list(column_data.index.names)
    ), "Columns column names in output do not match"
    normalised_compiled_metadata["resources"][0]["schema"]["fields"] = new_fields_column

    new_column_filename = f"bnetza_charging_columns_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][0]["name"] = (
        "model_draft." + new_column_filename if oep else new_column_filename
    )
    normalised_compiled_metadata["resources"][0]["path"] = f"{new_column_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][0]["dialect"] = {
            "delimiter": ",",
            "decimalSeparator": ".",
        }
        normalised_compiled_metadata["resources"][0]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][0].pop("encoding", None)

    # Point resource renaming
    point_resource_fields = normalised_compiled_metadata["resources"][1]["schema"][
        "fields"
    ]
    new_fields_point = []
    for field in point_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_point.append(field)

    assert set(f["name"] for f in new_fields_point) == set(
        list(point_data.columns) + list(point_data.index.names)
    ), "Socket column names in output do not match"
    normalised_compiled_metadata["resources"][1]["schema"]["fields"] = new_fields_point

    new_point_filename = f"bnetza_charging_points_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][1]["schema"]["foreignKeys"][0][
        "reference"
    ]["resource"] = (
        "model_draft." + new_column_filename if oep else new_column_filename
    )

    normalised_compiled_metadata["resources"][1]["name"] = (
        "model_draft." + new_point_filename if oep else new_point_filename
    )
    normalised_compiled_metadata["resources"][1]["path"] = f"{new_point_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][1]["dialect"] = {
            "delimiter": ",",
            "decimalSeparator": ".",
        }
        normalised_compiled_metadata["resources"][1]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][1].pop("encoding", None)

    # Operator resource renaming
    operator_resource_fields = normalised_compiled_metadata["resources"][2]["schema"][
        "fields"
    ]
    new_fields_operator = []
    for field in operator_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_operator.append(field)

    assert set(f["name"] for f in new_fields_operator) == set(
        list(operator_data.columns) + list(operator_data.index.names)
    ), "Operator column names in output do not match"
    normalised_compiled_metadata["resources"][2]["schema"][
        "fields"
    ] = new_fields_operator

    new_operator_filename = f"bnetza_operators_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][2]["name"] = (
        "model_draft." + new_operator_filename if oep else new_operator_filename
    )
    normalised_compiled_metadata["resources"][2][
        "path"
    ] = f"{new_operator_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][2]["dialect"] = {
            "delimiter": ",",
            "decimalSeparator": ".",
        }
        normalised_compiled_metadata["resources"][2]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][2].pop("encoding", None)

    normalised_compiled_metadata["resources"][0]["schema"]["foreignKeys"][0][
        "reference"
    ]["resource"] = (
        "model_draft." + new_operator_filename if oep else new_operator_filename
    )

    # Location resource renaming
    location_resource_fields = normalised_compiled_metadata["resources"][3]["schema"][
        "fields"
    ]
    new_fields_location = []
    for field in location_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_location.append(field)

    assert set(f["name"] for f in new_fields_location) == set(
        list(location_data.columns) + list(location_data.index.names)
    ), "Location column names in output do not match"
    normalised_compiled_metadata["resources"][3]["schema"][
        "fields"
    ] = new_fields_location

    new_location_filename = f"bnetza_locations_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][3]["name"] = (
        "model_draft." + new_location_filename if oep else new_location_filename
    )
    normalised_compiled_metadata["resources"][3][
        "path"
    ] = f"{new_location_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][3]["dialect"] = {
            "delimiter": ",",
            "decimalSeparator": ".",
        }
        normalised_compiled_metadata["resources"][3]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][3].pop("encoding", None)

    normalised_compiled_metadata["resources"][0]["schema"]["foreignKeys"][1][
        "reference"
    ]["resource"] = (
        "model_draft." + new_location_filename if oep else new_location_filename
    )

    normalised_compiled_metadata["name"] = OEP_NORMAL_FILENAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    normalised_compiled_metadata["id"] = OEP_NORMAL_FILENAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    normalised_compiled_metadata["description"] = (
        normalised_compiled_metadata["description"]
        + " Column names translated to english."
    )

    # Socket resource renaming
    socket_resource_fields = normalised_compiled_metadata["resources"][4]["schema"][
        "fields"
    ]
    new_fields_socket = []
    for field in socket_resource_fields:
        old_name = field["name"]
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        if "valueReference" in field.keys():
            new_refs = []
            for ref in field["valueReference"]:
                nr = ref
                nr["value"] = CONTENT_RENAME_TYPE[nr["value"]]
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields_socket.append(field)
    assert set(f["name"] for f in new_fields_socket) == set(
        list(socket_data.columns) + list(socket_data.index.names)
    ), "Socket column names in output do not match"
    normalised_compiled_metadata["resources"][4]["schema"]["fields"] = new_fields_socket

    new_socket_filename = f"bnetza_charging_sockets_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][4]["name"] = (
        "model_draft." + new_socket_filename if oep else new_socket_filename
    )
    normalised_compiled_metadata["resources"][4]["path"] = f"{new_socket_filename}.csv"

    # Compatibility resource renaming
    compatibility_resource_fields = normalised_compiled_metadata["resources"][5][
        "schema"
    ]["fields"]
    new_fields_compatibitliy = []
    for field in compatibility_resource_fields:
        old_name = field["name"]
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        if "valueReference" in field.keys():
            new_refs = []
            for ref in field["valueReference"]:
                nr = ref
                nr["value"] = CONTENT_RENAME_TYPE[nr["value"]]
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields_compatibitliy.append(field)
    assert set(f["name"] for f in new_fields_compatibitliy) == set(
        list(compatibility_data.columns) + list(compatibility_data.index.names)
    ), "Compatibility column names in output do not match"
    normalised_compiled_metadata["resources"][5]["schema"][
        "fields"
    ] = new_fields_compatibitliy

    new_compat_filename = f"bnetza_charging_compatibility_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][5]["name"] = (
        "model_draft." + new_compat_filename if oep else new_compat_filename
    )
    normalised_compiled_metadata["resources"][5]["path"] = f"{new_compat_filename}.csv"

    new_data = {
        "column": column_data,
        "point": point_data,
        "operator": operator_data,
        "location": location_data,
        "socket": socket_data,
        "compatibility": compatibility_data,
    }
    new_filenames = {
        "column": new_column_filename,
        "point": new_point_filename,
        "operator": new_operator_filename,
        "location": new_location_filename,
        "socket": new_socket_filename,
        "compatibility": new_compat_filename,
    }

    return new_data, new_filenames, normalised_compiled_metadata, (dd, mm, yyyy)


def get_renamed_annotated(download_date: tuple = None, oep=True):
    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = (
        annotate(download_date)
    )

    station_data = station_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        station_data["column_type"] = station_data["column_type"].str.replace(k, v)

    if oep:
        station_data.index.name = "id"

    station_resource_fields = station_compiled_metadata["resources"][0]["schema"][
        "fields"
    ]
    if oep:
        new_fields = [{"name": "id", "type": "integer", "description": "Unique ID"}]
    else:
        new_fields = []

    for field in station_resource_fields:
        old_name = field["name"]
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
        if "valueReference" in field.keys():
            new_refs = []
            for ref in field["valueReference"]:
                nr = ref
                nr["value"] = CONTENT_RENAME_TYPE[nr["value"]]
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields.append(field)
        column_set = set(list(station_data.columns))
        if oep:
            column_set.add("id")
    assert (
        set(f["name"] for f in new_fields) == column_set
    ), "Station column names in output do not match"

    station_compiled_metadata["resources"][0]["schema"]["fields"] = new_fields
    if oep:
        station_compiled_metadata["resources"][0]["schema"]["primaryKey"] = ["id"]
        station_compiled_metadata["resources"][0]["schema"]["foreignKeys"] = []
        station_compiled_metadata["resources"][0]["dialect"] = {
            "delimiter": ",",
            "decimalSeparator": ".",
        }
        station_compiled_metadata["resources"][0]["format"] = "PostgreSQL"
        station_compiled_metadata["resources"][0].pop("encoding", None)
    station_compiled_metadata["resources"][0]["name"] = (
        "model_draft." + station_compiled_metadata["resources"][0]["name"]
        if oep
        else station_compiled_metadata["resources"][0]["name"]
    )

    station_compiled_metadata["name"] = OEP_REGULAR_FILEANAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    station_compiled_metadata["id"] = OEP_REGULAR_FILEANAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    station_compiled_metadata["description"] = (
        station_compiled_metadata["description"]
        + " Column names translated to english."
    )
    return station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy)


def main():
    data, filenames, normalised_compiled_metadata, (dd, mm, yyyy) = (
        get_renamed_normalised(oep=OEP)
    )
    operational_name = Path(f"{OPERATIONAL_BASE}").joinpath(f"DE-{yyyy}{mm}{dd}-BNETZA-BNETZA") 
    if not operational_name.exists():
        operational_name.mkdir(exist_ok=True, parents=True)
    if not DEBUG:
        for element in data.keys():
            data[element].to_csv(
                operational_name.joinpath(f"{filenames[element]}.csv"),
                date_format="%Y-%m-%d %H:%M:%S",
            )

        with open(
            operational_name.joinpath(f"{OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json"),
            "w",
            encoding="utf8",
        ) as output:
            json.dump(
                normalised_compiled_metadata, output, indent=4, ensure_ascii=False
            )

    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = (
        get_renamed_annotated(oep=OEP)
    )

    if not (p := Path(f"{DEFAULT_DIR}")).exists():
        p.mkdir(parents=True, exist_ok=True)
    if DEBUG:
        station_data = station_data.head(10)
    station_data.to_csv(
        f"{DEFAULT_DIR}/{station_filename}.csv",
        index=OEP,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    with open(
        f"{DEFAULT_DIR}/{OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json",
        "w",
        encoding="utf8",
    ) as output:
        json.dump(station_compiled_metadata, output, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
