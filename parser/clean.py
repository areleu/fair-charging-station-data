# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from .load import get_raw
import pandas as pd
from os import path, mkdir

INPUT_METADATA_FILE = "metadata.yaml"

FAIRDIR = "fair"


def get_clean_data(filename: str | None = None, download_date: tuple | None = None):
    if not path.exists(FAIRDIR):
        mkdir(FAIRDIR)

    if filename is None:
        filename = get_raw(download_date)

    excel = pd.ExcelFile(filename)

    # Get current stand information
    dd, mm, yyyy = pd.read_excel(excel, nrows=10).iloc[6, 0].split(" ")[-1].split(".")
    filename = f"bnetza_charging_stations_{dd}_{mm}_{yyyy}"

    # Export as clean csv

    if not path.exists(f"{FAIRDIR}/{filename}.csv"):
        df = pd.read_excel(excel, header=10)
        # to measure duplicated capacity: df[df.duplicated()]["Nennleistung Ladeeinrichtung [kW]"].sum()
        df = df.drop_duplicates(ignore_index=True)

        # cleaning mixed types in column, looks clunky but dom't know a more transparent way, the data is just too heterogeneous.
        # Some column have string numbers, some use commas to separate decimals and some use points.
        df["Nennleistung Ladeeinrichtung [kW]"] = (
            df["Nennleistung Ladeeinrichtung [kW]"]
            .astype("string")
            .str.replace(",", ".")
            .astype(float)
        )
        df["Nennleistung Stecker1"] = (
            df["Nennleistung Stecker1"].astype("string").str.replace(",", ".")
        )
        df["Nennleistung Stecker2"] = (
            df["Nennleistung Stecker2"].astype("string").str.replace(",", ".")
        )
        df["Nennleistung Stecker3"] = (
            df["Nennleistung Stecker3"]
            .astype("string")
            .str.replace(",", ".")
            .str.replace("                 ", "NaN")
        )
        # The version from 16.07.2024 has a format issue in one entry that breaks this part of the script
        # because of this we strip
        df["Breitengrad"] = (
            df["Breitengrad"].astype("string").str.replace(",", ".").str.strip(".")
        )
        df["Längengrad"] = (
            df["Längengrad"].astype("string").str.replace(",", ".").astype(float)
        )
        # Replace cleaning steps when the source is changed
        # Drop all duplicates
        df["Inbetriebnahmedatum"] = pd.to_datetime(df["Inbetriebnahmedatum"])
        df.to_csv(
            f"{FAIRDIR}/{filename}.csv",
            sep=",",
            decimal=".",
            encoding="utf-8",
            date_format="%Y-%m-%d %H:%M:%S",
            index=False,
        )
    else:
        df = pd.read_csv(
            f"{FAIRDIR}/{filename}.csv", decimal=".", sep=",", encoding="utf-8"
        )
    # Datetime format
    df["Inbetriebnahmedatum"] = pd.to_datetime(df["Inbetriebnahmedatum"])

    # There is an incongruency between charging points declared and the ones given in the point list
    # TODO: Is it reasonable to replace the declared number with the actual values?
    # df["counted"] = (~ df["Steckertypen1"].isna()).astype(int) + (~ df["Steckertypen2"].isna()).astype(int) + (~ df["Steckertypen3"].isna()).astype(int) + (~ df["Steckertypen4"].isna()).astype(int)
    df["Anzahl Ladepunkte"] = (
        (~df["Steckertypen1"].isna()).astype(int)
        + (~df["Steckertypen2"].isna()).astype(int)
        + (~df["Steckertypen3"].isna()).astype(int)
        + (~df["Steckertypen4"].isna()).astype(int)
        + (~df["Steckertypen5"].isna()).astype(int)
        + (~df["Steckertypen6"].isna()).astype(int)
    )
    return df, filename, (dd, mm, yyyy)


def main():
    get_clean_data()  # If you want a specific date write the it in the forma (dd, mm, yyyy) ex: (1,2,2023)


if __name__ == "__main__":
    main()
