from download import get_raw
import pandas as pd
from os import path, mkdir

INPUT_METADATA_FILE = "metadata.yaml"

FAIRDIR = "fair"


def get_clean_data(download_date: tuple = None):
    if not path.exists(FAIRDIR):
        mkdir(FAIRDIR)
        
    raw = get_raw(download_date)
    excel = pd.ExcelFile(raw)

    # Get current stand information
    dd, mm, yyyy = pd.read_excel(excel, nrows=10).iloc[6, 0].split(" ")[-1].split(".")
    filename = f"bnetza_charging_stations_{dd}_{mm}_{yyyy}"

    # Export as clean csv

    if not path.exists(f"{FAIRDIR}/{filename}.csv"):
        df = pd.read_excel(excel, header=10)

       
        # cleaning mixed types in column, looks clunky but dom't know a more transparent way, the data is just too heterogeneous.
        # Some column have string numbers, some use commas to separate decimals and some use points.
        df["Anschlussleistung"] = df["Anschlussleistung"].astype("string").str.replace(",", ".").astype(float)
        df["P1 [kW]"] = df["P1 [kW]"].astype("string").str.replace(",", ".").astype(float)
        df["P2 [kW]"]  = df["P2 [kW]"] .astype("string").str.replace(",", ".").astype(float)
        df["P4 [kW]"] = df["P4 [kW]"].astype("string").str.replace(",", ".").str.replace("                 ", "NaN").astype(float)
        
        df["Breitengrad"]  = df["Breitengrad"].astype("string").str.replace(",", ".").astype(float)
        df["Längengrad"]  = df["Längengrad"].astype("string").str.replace(",", ".").astype(float)
        # Replace cleaning steps when the source is changed

        df.to_csv(
            f"{FAIRDIR}/{filename}.csv",
            sep=",",
            decimal=".",
            encoding="utf-8",
            index=False,
        )
    else:
        df = pd.read_csv(f"{FAIRDIR}/{filename}.csv", decimal=".", sep=",", encoding="utf-8")

    return df, filename, (dd, mm, yyyy)


def main():
    get_clean_data()  # If you want a specific date write the it in the forma (dd, mm, yyyy) ex: (1,2,2023)


if __name__ == "__main__":
    main()
