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

import requests
from datetime import datetime
from os import path, mkdir

URL = "https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.xlsx?__blob=publicationFile&v=44"
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
        raise IOError(f"The BNetzA API is not very realiable when using python. You have to manually download the file and save it as: {file_path}")
        raw = download_raw(URL)
        with open(file_path, "wb") as file:
            file.write(raw)
    return file_path


def main():
    get_raw()


if __name__ == "__main__":
    main()
