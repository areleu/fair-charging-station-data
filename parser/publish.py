# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

# %%
import json
from random import randint
from getpass import getpass
from os import environ
from .rename import get_renamed_annotated
import requests as req
from oep_client import OepClient
from json import loads, dumps
import pandas as pd
import numpy as np

OEPDIR_DEFAULT = "oep_default"

topic = "model_draft"
token = environ.get("OEP_API_TOKEN") or getpass("Enter your OEP API token:")

cli = OepClient(token=token, default_schema=topic)
# %%
station_data, station_filename, station_compiled_metadata, (dd, mm, yyyy) = (
    get_renamed_annotated(download_date=(1, 10, 2023))
)
table = station_filename
# %%
table_schema = {
    "columns": [
        {"name": "id", "data_type": "bigserial", "primary_key": True},
        {"name": "operator", "data_type": "varchar(100)", "is_nullable": False},
        {"name": "thoroughfare", "data_type": "varchar(50)", "is_nullable": False},
        {"name": "locator_designator", "data_type": "varchar(13)"},
        {
            "name": "address_supplement",
            "data_type": "varchar(300)",
            "is_nullable": True,
        },
        {"name": "postcode", "data_type": "integer", "is_nullable": False},
        {"name": "post_name", "data_type": "varchar(40)"},
        {"name": "federal_state", "data_type": "varchar(30)"},
        {"name": "county", "data_type": "varchar(50)"},
        {"name": "latitude", "data_type": "decimal"},
        {"name": "longitude", "data_type": "decimal"},
        {"name": "commissioning_date", "data_type": "date"},
        {"name": "net_capacity", "data_type": "float(14)"},
        {"name": "column_type", "data_type": "varchar(10)"},
        {"name": "charger_amount", "data_type": "smallint"},
        {"name": "charger_type_1", "data_type": "varchar(80)"},
        {"name": "charger_power_1", "data_type": "float(14)"},
        {"name": "charger_public_key_1", "data_type": "text"},
        {"name": "charger_type_2", "data_type": "varchar(80)"},
        {"name": "charger_power_2", "data_type": "float(14)"},
        {"name": "charger_public_key_2", "data_type": "text"},
        {"name": "charger_type_3", "data_type": "varchar(80)"},
        {"name": "charger_power_3", "data_type": "float(14)"},
        {"name": "charger_public_key_3", "data_type": "text"},
        {"name": "charger_type_4", "data_type": "varchar(80)"},
        {"name": "charger_power_4", "data_type": "float(14)"},
        {"name": "charger_public_key_4", "data_type": "text"},
    ]
}
# %%
my_list = [
    "operator",
    "thoroughfare",
    "locator_designator",
    "address_supplement",
    "postcode",
    "post_name",
    "federal_state",
    "county",
    "latitude",
    "longitude",
    "commissioning_date",
    "net_capacity",
    "column_type",
    "charger_amount",
    "charger_type_1",
    "charger_power_1",
    "charger_public_key_1",
    "charger_type_2",
    "charger_power_2",
    "charger_public_key_2",
    "charger_type_3",
    "charger_power_3",
    "charger_public_key_3",
    "charger_type_4",
    "charger_power_4",
    "charger_public_key_4",
    "charger_type_5",
    "charger_power_5",
    "charger_public_key_5",
    "charger_type_6",
    "charger_power_6",
    "charger_public_key_6",
]
# %%
station_data["commissioning_date"] = station_data["commissioning_date"].dt.strftime(
    "%Y-%m-%d"
)
station_data = station_data.where(pd.notnull(station_data), None)
station_data["charger_power_2"] = station_data["charger_power_2"].replace(np.nan, None)
station_data["charger_power_3"] = station_data["charger_power_3"].replace(np.nan, None)
station_data["charger_power_4"] = station_data["charger_power_4"].replace(np.nan, None)
data = station_data[my_list].reset_index().to_dict(orient="records")
# %%
print(table)
cli.create_table(table, table_schema)
# %%
cli.insert_into_table(table, data)
metadata = cli.set_metadata(table, station_compiled_metadata)
# %%
# cli.drop_table(table)

# %%
