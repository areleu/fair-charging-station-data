import frictionless as fl
from frictionless.resources import TableResource

PROFILE = "https://raw.githubusercontent.com/OpenEnergyPlatform/oemetadata/develop/metadata/v160/schema.json"
#resource = fl.Resource.describe(source="oep/bnetza_charging_stations_01_02_2023.csv")

package = fl.Package(source="oep/bnetza_charging_stations_normalised_01_02_2023.json", profile=PROFILE)

print(package.validate())
# package.to_descriptor(validate=True)

# types to SQL

# Add description

# https://github.com/OpenEnergyPlatform/academy/blob/production/docs/tutorials/upload/OEP_Upload_Process_Data_and_Metadata_oem2orm.ipynb
# https://openenergy-platform.org/dataedit/view/model_draft/
# https://modex.rl-institut.de/create_table/
# https://github.com/OpenEnergyPlatform/academy/blob/production/docs/tutorials/upload/OEP_Upload_Process_Data_and_Metadata_oem2orm.ipynb

# add schema to name model_schema
# Add primary and foreign keys, empty, add a primary key id
# Add dialect, delimiter and decimalSeparator