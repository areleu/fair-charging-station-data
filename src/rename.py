# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from normalise import get_normalised_data
from annotate import annotate
import json
from os import mkdir, path

DEBUG = False
OEP = True

OEPDIR_DEFAULT = "oep_default"
OEPDIR_NORMAL = "oep_normal"
OEP_NORMAL_FILENAME = "bnetza_charging_stations_normalised_{dd}_{mm}_{yyyy}"
OEP_REGULAR_FILEANAME = "bnetza_charging_stations_{dd}_{mm}_{yyyy}"

COLUMN_RENAME = {
    "Betreiber": "operator",
    "Straße" : "thoroughfare",
    "Hausnummer": "locator_designator",
    "Adresszusatz": "address_supplement",
    "Postleitzahl": "postcode",
    "Ort": "post_name",
    "Bundesland": "federal_state",
    "Kreis/kreisfreie Stadt": "county",
    "Breitengrad": "latitude",
    "Längengrad": "longitude",
    "Inbetriebnahmedatum": "comissioning_date",
    "Anschlussleistung": "net_capacity",
    "Art der Ladeeinrichung": "column_type",
    "Anzahl Ladepunkte": "charger_amount",
    "Steckertypen1": "charger_type_1",
    "P1 [kW]": "charger_power_1",
    "Public Key1": "charger_public_key_1",
    "Steckertypen2": "charger_type_2",
    "P2 [kW]": "charger_power_2",
    "Public Key2": "charger_public_key_2",
    "Steckertypen3": "charger_type_3",
    "P3 [kW]": "charger_power_3",
    "Public Key3": "charger_public_key_3",
    "Steckertypen4": "charger_type_4",
    "P4 [kW]": "charger_power_4",
    "Public Key4": "charger_public_key_4",
    "Steckertypen": "charger_type",
    "Leistungskapazität": "charger_power",
    "PublicKey": "public_key"
}

CONTENT_RENAME_TYPE = {
    "Normalladeeinrichtung": "regular",
    "Schnellladeeinrichtung": "fast"
}

COLUMN_DATATYPE = {
    "Postleitzahl": "integer",
    "Breitengrad": "float",
    "Längengrad": "float",
    "Anschlussleistung": "float",
    "Anzahl Ladepunkte": "integer",
    "P1 [kW]": "float",
    "P2 [kW]": "float",
    "P3 [kW]": "float",
    "P4 [kW]": "float",
    "Leistungskapazität": "float",
    "id": "integer",
    "column_id": "integer",
    "Inbetriebnahmedatum": "date"
}

def get_renamed_normalised(download_date: tuple = None, oep=True):
    column_data, socket_data, operator_data, location_data, _, _, _, _, normalised_compiled_metadata, (dd, mm, yyyy)= get_normalised_data(download_date)

    column_data = column_data.rename(columns=COLUMN_RENAME)
    socket_data = socket_data.rename(columns=COLUMN_RENAME)
    operator_data = operator_data.rename(columns=COLUMN_RENAME)
    location_data = location_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        column_data["column_type"] = column_data["column_type"].str.replace(k,v)

    # Column resource renaming
    column_resource_fields = normalised_compiled_metadata["resources"][0]["schema"]["fields"]
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
    assert set(f["name"] for f in new_fields_column) == set(list(column_data.columns) + list(column_data.index.names)), "Columns column names in output do not match"
    normalised_compiled_metadata["resources"][0]["schema"]["fields"] = new_fields_column

    new_column_filename = f"bnetza_charging_columns_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][0]["name"] = "model_draft." + new_column_filename if oep else new_column_filename
    normalised_compiled_metadata["resources"][0]["path"] = f"{new_column_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][0]["dialect"] = { "delimiter": ",", "decimalSeparator": "."}
        normalised_compiled_metadata["resources"][0]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][0].pop("encoding", None)

    # Socket resource renaming
    socket_resource_fields = normalised_compiled_metadata["resources"][1]["schema"]["fields"]
    new_fields_socket = []
    for field in socket_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_socket.append(field)

    assert set(f["name"] for f in new_fields_socket) == set(list(socket_data.columns) + list(socket_data.index.names)), "Socket column names in output do not match"
    normalised_compiled_metadata["resources"][1]["schema"]["fields"] = new_fields_socket

    new_socket_filename = f"bnetza_charging_sockets_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][1]["schema"]["foreignKeys"][0]["reference"]["resource"] = "model_draft."+ new_column_filename if oep else new_column_filename

    normalised_compiled_metadata["resources"][1]["name"] = "model_draft."+ new_socket_filename if oep else new_socket_filename
    normalised_compiled_metadata["resources"][1]["path"] = f"{new_socket_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][1]["dialect"] = { "delimiter": ",", "decimalSeparator": "."}
        normalised_compiled_metadata["resources"][1]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][1].pop("encoding", None)

    # Operator resource renaming
    operator_resource_fields = normalised_compiled_metadata["resources"][2]["schema"]["fields"]
    new_fields_operator = []
    for field in operator_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_operator.append(field)

    assert set(f["name"] for f in new_fields_operator) == set(list(operator_data.columns) + list(operator_data.index.names)), "Operator column names in output do not match"
    normalised_compiled_metadata["resources"][2]["schema"]["fields"] = new_fields_operator

    new_operator_filename = f"bnetza_operators_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][2]["name"] = "model_draft."+ new_operator_filename if oep else new_operator_filename
    normalised_compiled_metadata["resources"][2]["path"] = f"{new_operator_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][2]["dialect"] = { "delimiter": ",", "decimalSeparator": "."}
        normalised_compiled_metadata["resources"][2]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][2].pop("encoding", None)

    normalised_compiled_metadata["resources"][0]["schema"]["foreignKeys"][0]["reference"]["resource"] = "model_draft."+ new_operator_filename if oep else new_operator_filename

    # Location resource renaming
    location_resource_fields = normalised_compiled_metadata["resources"][3]["schema"]["fields"]
    new_fields_location = []
    for field in location_resource_fields:
        old_name = field["name"]
        if oep:
            field["type"] = COLUMN_DATATYPE.get(old_name, field.get("type", "string"))
            if field["name"] == "id":
                field.pop("constraints", None)
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_location.append(field)

    assert set(f["name"] for f in new_fields_location) == set(list(location_data.columns) + list(location_data.index.names)), "Location column names in output do not match"
    normalised_compiled_metadata["resources"][3]["schema"]["fields"] = new_fields_location

    new_location_filename = f"bnetza_locations_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][3]["name"] = "model_draft."+ new_location_filename if oep else new_location_filename
    normalised_compiled_metadata["resources"][3]["path"] = f"{new_location_filename}.csv"

    if oep:
        normalised_compiled_metadata["resources"][3]["dialect"] = { "delimiter": ",", "decimalSeparator": "."}
        normalised_compiled_metadata["resources"][3]["format"] = "PostgreSQL"
        normalised_compiled_metadata["resources"][3].pop("encoding", None)

    normalised_compiled_metadata["resources"][0]["schema"]["foreignKeys"][1]["reference"]["resource"] = "model_draft."+ new_location_filename if oep else new_location_filename

    normalised_compiled_metadata["name"] = OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)
    normalised_compiled_metadata["id"] = OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)
    normalised_compiled_metadata["description"] = normalised_compiled_metadata["description"] + " Column names translated to english."

    return column_data, socket_data, operator_data, location_data, new_column_filename, new_socket_filename, new_operator_filename, new_location_filename, normalised_compiled_metadata, (dd, mm, yyyy)

def get_renamed_annotated(download_date: tuple = None, oep=True):
    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = annotate(download_date)

    station_data = station_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        station_data["column_type"] = station_data["column_type"].str.replace(k,v)

    if oep:
        station_data.index.name = "id"


    station_resource_fields = station_compiled_metadata["resources"][0]["schema"]["fields"]
    if oep:
        new_fields = [{"name": "id", "type": "integer", "description": "Unique ID" }]
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
    assert set(f["name"] for f in new_fields) ==column_set, "Station column names in output do not match"

    station_compiled_metadata["resources"][0]["schema"]["fields"] = new_fields
    if oep:
        station_compiled_metadata["resources"][0]["schema"]["primaryKey"] = ["id"]
        station_compiled_metadata["resources"][0]["schema"]["foreignKeys"] = []
        station_compiled_metadata["resources"][0]["dialect"] = { "delimiter": ",", "decimalSeparator": "."}
        station_compiled_metadata["resources"][0]["format"] = "PostgreSQL"
        station_compiled_metadata["resources"][0].pop("encoding", None)
    station_compiled_metadata["resources"][0]["name"] = "model_draft." + station_compiled_metadata["resources"][0]["name"] if oep else station_compiled_metadata["resources"][0]["name"]

    station_compiled_metadata["name"] = OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)
    station_compiled_metadata["id"] = OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)
    station_compiled_metadata["description"] = station_compiled_metadata["description"] + " Column names translated to english."
    return station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy)

def main():
    column_data, socket_data, operator_data, location_data, column_filename, socket_filename, operator_filename, location_filename, normalised_compiled_metadata, (dd, mm, yyyy) = get_renamed_normalised(oep=OEP)
    if not path.exists(f"{OEPDIR_NORMAL}"):
        mkdir(OEPDIR_NORMAL)
    if not DEBUG:
        column_data.to_csv(f"{OEPDIR_NORMAL}/{column_filename}.csv")
        socket_data.to_csv(f"{OEPDIR_NORMAL}/{socket_filename}.csv")
        operator_data.to_csv(f"{OEPDIR_NORMAL}/{operator_filename}.csv")
        location_data.to_csv(f"{OEPDIR_NORMAL}/{location_filename}.csv")

        with open(f"{OEPDIR_NORMAL}/{OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
            json.dump(normalised_compiled_metadata, output, indent=4, ensure_ascii=False)

    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = get_renamed_annotated(oep=OEP)

    if not path.exists(f"{OEPDIR_DEFAULT}"):
        mkdir(OEPDIR_DEFAULT)
    if DEBUG:
        station_data = station_data.head(10)
    station_data.to_csv(f"{OEPDIR_DEFAULT}/{station_filename}.csv", index=OEP)

    with open(f"{OEPDIR_DEFAULT}/{OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(station_compiled_metadata, output, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()