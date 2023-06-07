from normalise import get_normalised_data
from annotate import annotate

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
    column_data, socket_data, column_filename, socket_filename, normalised_compiled_metadata, (dd, mm, yyyy) = get_normalised_data(download_date)

    column_data = column_data.rename(columns=COLUMN_RENAME)
    socket_data = socket_data.rename(columns=COLUMN_RENAME)

    for k, v in CONTENT_RENAME_TYPE.items():
        column_data["column_type"] = column_data["column_type"].str.replace(k,v)

    column_resource = normalised_compiled_metadata["resources"][0]
    socket_resource = normalised_compiled_metadata["resources"][0]

    return column_data, socket_data, column_filename, socket_filename, normalised_compiled_metadata, (dd, mm, yyyy)

def main():
    column_data, socket_data, column_filename, socket_filename, normalised_compiled_metadata, (dd, mm, yyyy) = get_renamed_normalised()
    # station_data, station_filename, station_compiled_metadata, (_, _, _) = annotate()

if __name__ == "__main__":
    main()