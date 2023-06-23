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
        df = df.drop_duplicates(ignore_index=True)


        # cleaning mixed types in column, looks clunky but dom't know a more transparent way, the data is just too heterogeneous.
        # Some column have string numbers, some use commas to separate decimals and some use points.
        df["Anschlussleistung"] = df["Anschlussleistung"].astype("string").str.replace(",", ".").astype(float)
        df["P1 [kW]"] = df["P1 [kW]"].astype("string").str.replace(",", ".").astype(float)
        df["P2 [kW]"]  = df["P2 [kW]"] .astype("string").str.replace(",", ".").astype(float)
        df["P4 [kW]"] = df["P4 [kW]"].astype("string").str.replace(",", ".").str.replace("                 ", "NaN").astype(float)

        df["Breitengrad"]  = df["Breitengrad"].astype("string").str.replace(",", ".").astype(float)
        df["Längengrad"]  = df["Längengrad"].astype("string").str.replace(",", ".").astype(float)
        # Replace cleaning steps when the source is changed
        # Drop all duplicates

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
