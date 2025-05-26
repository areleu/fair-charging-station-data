# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from pathlib import Path
from normalise import get_normalised_data
from annotate import annotate
import json
from pandas import DataFrame

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

COLUMN_OPERATION_STATUS = {
    "In Betrieb": "operational",
    "In Wartung": "maintenance",
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

OPENING_HOURS_MAP = {"247": "24/7", "Eingeschränkt": "Limited"}


def rename_fields(resource_fields, data_columns, oep):
    """Rename fields in the resource schema."""
    new_fields = []
    for field in resource_fields:
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
                nr["value"] = CONTENT_RENAME_TYPE.get(nr["value"], nr["value"])
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields.append(field)

    assert set(f["name"] for f in new_fields) == set(data_columns), (
        "Column names in output do not match"
    )
    return new_fields


def update_metadata(resource, filename, oep):
    """Update metadata for a resource."""
    resource["name"] = f"model_draft.{filename}" if oep else filename
    resource["path"] = f"{filename}.csv"
    if oep:
        resource["dialect"] = {"delimiter": ",", "decimalSeparator": "."}
        resource["format"] = "PostgreSQL"
        resource.pop("encoding", None)


def process_resource(resource, data, filename, oep):
    """Process a single resource by renaming fields and updating metadata."""
    resource["schema"]["fields"] = rename_fields(
        resource["schema"]["fields"],
        list(data.columns) + list(data.index.names),
        oep,
    )
    update_metadata(resource, filename, oep)


def rename_data_columns(data):
    """Rename columns in the data using COLUMN_RENAME."""
    for key in data.keys():
        data[key] = data[key].rename(columns=COLUMN_RENAME)
        if key == "column":
            for k, v in CONTENT_RENAME_TYPE.items():
                data[key]["column_type"] = data[key]["column_type"].str.replace(k, v)
            for k, v in COLUMN_OPERATION_STATUS.items():
                data[key]["status"] = data[key]["status"].str.replace(k, v)
        if key == "facility":
            data[key]["opening_times"] = data[key]["opening_times"].apply(
                lambda x: OPENING_HOURS_MAP.get(x, x)
            )


def process_all_resources(data, normalised_compiled_metadata, oep, dd, mm, yyyy):
    """Process all resources and return updated filenames."""
    resource_filenames = [
        ("column", f"bnetza_charging_columns_{dd}_{mm}_{yyyy}"),
        ("facility", f"bnetza_charging_facilities_{dd}_{mm}_{yyyy}"),
        ("point", f"bnetza_charging_points_{dd}_{mm}_{yyyy}"),
        ("operator", f"bnetza_operators_{dd}_{mm}_{yyyy}"),
        ("location", f"bnetza_locations_{dd}_{mm}_{yyyy}"),
        ("socket", f"bnetza_charging_sockets_{dd}_{mm}_{yyyy}"),
        ("compatibility", f"bnetza_charging_compatibility_{dd}_{mm}_{yyyy}"),
    ]

    for i, (key, filename) in enumerate(resource_filenames):
        process_resource(
            normalised_compiled_metadata["resources"][i], data[key], filename, oep
        )

    return {key: filename for key, filename in resource_filenames}


def get_renamed_normalised(download_date: tuple = None, oep=True):
    data, filenames, normalised_compiled_metadata, (dd, mm, yyyy) = get_normalised_data(
        download_date
    )

    # Rename data columns
    rename_data_columns(data)

    # Process all resources
    new_filenames = process_all_resources(
        data, normalised_compiled_metadata, oep, dd, mm, yyyy
    )

    # Update metadata
    normalised_compiled_metadata["name"] = OEP_NORMAL_FILENAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    normalised_compiled_metadata["id"] = OEP_NORMAL_FILENAME.format(
        mm=mm, dd=dd, yyyy=yyyy
    )
    normalised_compiled_metadata["description"] += "Column names translated to english."

    return data, new_filenames, normalised_compiled_metadata, (dd, mm, yyyy)


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
    assert set(f["name"] for f in new_fields) == column_set, (
        "Station column names in output do not match"
    )

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
    operational_name = Path(f"{OPERATIONAL_BASE}").joinpath(
        f"DE-{yyyy}{mm}{dd}-BNETZA-BNETZA"
    )
    if not operational_name.exists():
        operational_name.mkdir(exist_ok=True, parents=True)
    if not DEBUG:
        for element in data.keys():
            data[element].to_csv(
                operational_name.joinpath(f"{filenames[element]}.csv"),
                date_format="%Y-%m-%d %H:%M:%S",
            )

        with open(
            operational_name.joinpath(
                f"{OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json"
            ),
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
