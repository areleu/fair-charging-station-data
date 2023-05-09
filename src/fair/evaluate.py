import requests
import json 
import jsonschema_rs
from pathlib import Path
from clean import get_clean_data, FAIRDIR
from normalise import NORMALIZED_FILENAME
METADATA_GENERIC = "https://raw.githubusercontent.com/OpenEnergyPlatform/oemetadata/develop/metadata/{}/schema.json"

METADATA_VERSIONS = ["v130", "v140", "v141", "v150", "v151", "v152"]

LATEST_METADATA = "v152"

LOCAL_PATH = "metadata/metadata_{}.json"

def get_metadata_schema(source, local):
    if not Path(local).exists():
        response = requests.get(source).json()
        with open(local, "w") as fp:
            json.dump(response, fp)
        schema = response
    else:
        with open(local, "r") as fp:
            schema =json.load(fp)
    return schema

def load_metadata(version):
    source = METADATA_GENERIC.format(version)
    local = LOCAL_PATH.format(version)
    Path(local).parent.mkdir(exist_ok=True, parents=True)
    metadata = get_metadata_schema(source, local)
    return metadata

def get_metadata(source):
    with open(source, "r") as fp:
        package =json.load(fp)
    return package

def test_compilance(metadata, version):
    metadata_object = get_metadata(metadata)
    metadata = load_metadata(version)
    validator = jsonschema_rs.JSONSchema(metadata)
    report = []
    valid_schema = validator.is_valid(metadata_object)
    if not valid_schema:
        for error in validator.iter_errors(metadata_object):
            error_dict = {
                "message": error.message,
                "schema_path": error.schema_path,
                "instance_path": error.instance_path
            }
            report.append(error_dict)
        name = Path(metadata).stem
        output_file = Path(f"tests/reports/report_{name}_{version}.json")
        output_file.parent.mkdir(exist_ok=True, parents=True)
        with open(output_file, "w") as fp:
            json.dump(report, fp,indent=4, sort_keys=False)

        assert valid_schema, f"The file {Path(metadata).name} is not valid agianst oemetadata {version}"

def main():
    _, filename, (dd, mm, yyyy) = get_clean_data()

    original = f"{FAIRDIR}/{filename}.json"
    normalised = f"{FAIRDIR}/{NORMALIZED_FILENAME.format(mm=mm, dd=dd, yyyy=yyyy)}.json"

    for d in [original, normalised]:
        test_compilance(d, "v152")

if __name__== "__main__":
    main()
