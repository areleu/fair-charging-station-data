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

from clean import get_clean_data, FAIRDIR
import frictionless as fl
import yaml
from collections import OrderedDict
from omi.dialects.oep.dialect import OEP_V_1_5_Dialect
import json
from os import mkdir, path

INPUT_METADATA_FILE = "metadata.yaml"
if not path.exists(INPUT_METADATA_FILE):
    INPUT_METADATA_FILE = "src/fair/metadata.yaml"


def annotate(download_date: tuple = None):
    if not path.exists(f"{FAIRDIR}"):
        mkdir(FAIRDIR)

    df, filename, (dd, mm, yyyy) = get_clean_data(download_date)  # If you want a specific date write the it in the forma (dd, mm, yyyy) ex: (1,2,2023)

    # get current file schema
    schema = fl.Schema.describe(f"{FAIRDIR}/{filename}.csv")
    dictionary = schema.to_dict()

    # get annotated fields
    with open(INPUT_METADATA_FILE, "r", encoding="utf-8") as f:
        annotations = yaml.safe_load(f)

    # assert field similarity
    if not set(f["name"] for f in dictionary["fields"]) == set(
        f["name"] for f in annotations["resources"][0]["schema"]["fields"]
    ):
        diffs = set(f["name"] for f in dictionary["fields"]) - set(
            f["name"] for f in annotations["resources"][0]["schema"]["fields"]
        )
        print(f"The fields {diffs} are no longer up to date.")

    # update fields
    fields = OrderedDict(
        {f["name"]: f for f in annotations["resources"][0]["schema"]["fields"]}
    )
    for new in dictionary["fields"]:
        fields[new["name"]].update(new)
    values = [v for v in fields.values()]

    # Update schema and annotations
    annotations["resources"][0]["name"] = f"bnetza_charging_stations_{dd}_{mm}_{yyyy}"
    annotations["resources"][0]["path"] = f"{filename}.csv"
    annotations["resources"][0]["format"] = "csv"
    annotations["resources"][0]["encoding"] = "utf-8"
    annotations["resources"][0]["schema"]["fields"] = values

    # Update name
    annotations["name"] = annotations["name"] + f"_{dd}_{mm}_{yyyy}"
    # Update publication date
    annotations["publicationDate"] = f"{yyyy}-{mm}-{dd}"

    dialect1_5 = OEP_V_1_5_Dialect()
    compiled_metadata = dialect1_5.compile(annotations)

    return df, filename, compiled_metadata, (dd, mm, yyyy)



def main():
    _, filename, compiled_metadata, (_, _, _) = annotate()

    with open(f"{FAIRDIR}/{filename}.json", "w", encoding="utf8") as output:
        json.dump(compiled_metadata, output, indent=4, ensure_ascii=False)
if __name__ == "__main__":
    main()
