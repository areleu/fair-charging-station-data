import requests
from datetime import datetime
from os import path, mkdir
from io import BytesIO
from openpyxl import load_workbook

URL = "https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx?__blob=publicationFile&v=43"
FILENAME = "bnetza_charging_stations_raw_{MM}_{YYYY}.xlsx"
DATA_DIRECTORY = "data"


def download_raw(url: str) -> str:
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(
            f"There was an error downloading the file, code: {response.status_code}, reason: {response.reason}, text: {response.text}"
        )
    raw = response.content
    return raw


def get_raw(dd_mm_yyyy: tuple = None):
    if dd_mm_yyyy is None:
        _, mm, yyyy = datetime.today().strftime("%d-%m-%Y").split("-")
    else:
        _, mm, yyyy = dd_mm_yyyy
    file_path = path.join(DATA_DIRECTORY, FILENAME.format(MM=mm, YYYY=yyyy))
    if not path.exists(DATA_DIRECTORY):
        mkdir(DATA_DIRECTORY)
    if not path.exists(file_path):
        # raw = BytesIO(download_raw(URL))
        # wb = load_workbook(raw)
        # wb.save(file_path)
        raw = download_raw(URL)
        with open(file_path, "wb") as file:
            file.write(raw)
    return file_path


def main():
    get_raw()


if __name__ == "__main__":
    main()