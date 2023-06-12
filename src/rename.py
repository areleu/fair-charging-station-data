from normalise import get_normalised_data
from annotate import annotate
import json
from os import mkdir, path

OEPDIR = "oep"
OEP_NORMAL_FILENAME = "bnetza_charging_stations_normalised_oep_{dd}_{mm}_{yyyy}"
OEP_REGULAR_FILEANAME = "bnetza_charging_stations_oep_{dd}_{mm}_{yyyy}"

COLUMN_RENAME = {
    "Betreiber": "operator",
    "Straße" : "street",
    "Hausnummer": "address_number",
    "Adresszusatz": "address_supplement",
    "Postleitzahl": "postcode",
    "Ort": "municipality",
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

def get_renamed_normalised(download_date: tuple = None):
    column_data, socket_data, _, _, normalised_compiled_metadata, (dd, mm, yyyy) = get_normalised_data(download_date)

    column_data = column_data.rename(columns=COLUMN_RENAME)
    socket_data = socket_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        column_data["column_type"] = column_data["column_type"].str.replace(k,v)

    # Column resource renaming
    column_resource_fields = normalised_compiled_metadata["resources"][0]["schema"]["fields"]
    new_fields_column = []
    for field in column_resource_fields:
        old_name = field["name"] 
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
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

    new_column_filename = f"bnetza_charging_columns_oep_{dd}_{mm}_{yyyy}"

    normalised_compiled_metadata["resources"][0]["name"] = new_column_filename
    normalised_compiled_metadata["resources"][0]["path"] = new_column_filename
    
    # Socket resource renaming
    socket_resource_fields = normalised_compiled_metadata["resources"][1]["schema"]["fields"]
    new_fields_socket = []
    for field in socket_resource_fields:
        old_name = field["name"] 
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        new_fields_socket.append(field)
        
    
    new_socket_filename = f"bnetza_charging_sockets_oep_{dd}_{mm}_{yyyy}"

    assert set(f["name"] for f in new_fields_socket) == set(list(socket_data.columns) + list(socket_data.index.names)), "Socket column names in output do not match"
    
    normalised_compiled_metadata["resources"][1]["schema"]["fields"] = new_fields_socket
    normalised_compiled_metadata["resources"][1]["schema"]["foreignKeys"][0]["reference"]["resource"] = new_column_filename

    normalised_compiled_metadata["resources"][1]["name"] = new_socket_filename
    normalised_compiled_metadata["resources"][1]["path"] = new_socket_filename

    normalised_compiled_metadata["name"] = OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)
    normalised_compiled_metadata["id"] = OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)
    normalised_compiled_metadata["description"] = normalised_compiled_metadata["description"] + " Column names translated to english."
    
    return column_data, socket_data, new_column_filename, new_socket_filename, normalised_compiled_metadata, (dd, mm, yyyy)

def get_renamed_annotated(download_date: tuple = None):
    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = annotate(download_date)

    station_data = station_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        station_data["column_type"] = station_data["column_type"].str.replace(k,v)

    station_resource_fields = station_compiled_metadata["resources"][0]["schema"]["fields"]
    new_fields = []

    for field in station_resource_fields:
        old_name = field["name"] 
        field["name"] = COLUMN_RENAME.get(old_name, old_name)
        if "valueReference" in field.keys():
            new_refs = []
            for ref in field["valueReference"]:
                nr = ref
                nr["value"] = CONTENT_RENAME_TYPE[nr["value"]]
                new_refs.append(nr)
            field["valueReference"] = new_refs
        new_fields.append(field)
    assert set(f["name"] for f in new_fields) == set(list(station_data.columns)), "Station column names in output do not match"
    
    station_compiled_metadata["resources"][0]["schema"]["fields"] = new_fields

    station_compiled_metadata["name"] = OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)
    station_compiled_metadata["id"] = OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)
    station_compiled_metadata["description"] = station_compiled_metadata["description"] + " Column names translated to english."
    return station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy)

def main():
    column_data, socket_data, column_filename, socket_filename, normalised_compiled_metadata, (dd, mm, yyyy) = get_renamed_normalised()
    if not path.exists(f"{OEPDIR}"):
        mkdir(OEPDIR)

    column_data.to_csv(f"{OEPDIR}/{column_filename}.csv")
    socket_data.to_csv(f"{OEPDIR}/{socket_filename}.csv")

    with open(f"{OEPDIR}/{OEP_NORMAL_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(normalised_compiled_metadata, output, indent=4, ensure_ascii=False)

    station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = get_renamed_annotated()

    station_data.to_csv(f"{OEPDIR}/{station_filename}.csv", index=False)

    with open(f"{OEPDIR}/{OEP_REGULAR_FILEANAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json", "w", encoding="utf8") as output:
        json.dump(station_compiled_metadata, output, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()