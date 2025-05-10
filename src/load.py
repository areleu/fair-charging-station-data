# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

from datetime import datetime
from os import path, mkdir

FILENAME = "bnetza_charging_stations_raw_{MM}_{YYYY}.xlsx"
DATA_DIRECTORY = "data"


def get_raw(dd_mm_yyyy: tuple = None):
    if dd_mm_yyyy is None:
        _, mm, yyyy = datetime.today().strftime("%d-%m-%Y").split("-")
    else:
        _, mm, yyyy = dd_mm_yyyy
    file_path = path.join(DATA_DIRECTORY, FILENAME.format(MM=mm, YYYY=yyyy))
    if not path.exists(DATA_DIRECTORY):
        mkdir(DATA_DIRECTORY)
    if not path.exists(file_path):
        file_path = path.join("..", file_path)
    if not path.exists(file_path):
        raise IOError(f"File {file_path} not found. You have to manually download the file from the BNetzA webpage(see README) and save it as: {file_path}")
    return file_path


def main():
    get_raw()


if __name__ == "__main__":
    main()
