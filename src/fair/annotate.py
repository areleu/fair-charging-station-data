from clean import get_clean_data, FAIRDIR
import frictionless as fl
import yaml
from collections import OrderedDict
from omi.dialects.oep.dialect import OEP_V_1_5_Dialect
import json
from os import mkdir, path

INPUT_METADATA_FILE = "metadata.yaml"


def annotate():
    if not path.exists(f"{FAIRDIR}"):
        mkdir(FAIRDIR)

    _, filename, (dd, mm, yyyy) = get_clean_data()  # If you want a specific date write the it in the forma (dd, mm, yyyy) ex: (1,2,2023)

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
    annotations["resources"][0]["path"] = filename
    annotations["resources"][0]["format"] = "CSV"
    annotations["resources"][0]["encoding"] = "UTF-8"
    annotations["resources"][0]["schema"]["fields"] = values

    # Update publication date
    annotations["publicationDate"] = f"{yyyy}-{mm}-{dd}"

    dialect1_5 = OEP_V_1_5_Dialect()
    compiled = dialect1_5.compile(annotations)
    with open(f"{FAIRDIR}/{filename}.json", "w", encoding="utf8") as output:
        json.dump(compiled, output, indent=4, ensure_ascii=False)


def main():
    annotate()


if __name__ == "__main__":
    main()
