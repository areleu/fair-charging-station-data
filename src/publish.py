# SPDX-FileCopyrightText: Copyright (c) 2023 German Aerospace Center (DLR)
# SPDX-License-Identifier: BSD-3-Clause

# %%
from rename import get_renamed_annotated, get_renamed_normalised
from oem2orm import oep_oedialect_oem2orm as oem2orm
import os
import pandas as pd
import getpass
# %%
oem2orm.setup_logger()
# %%
db = oem2orm.setup_db_connection()
# %%
metadata_folder = oem2orm.select_oem_dir(oem_folder_name=os.path.abspath("../oep"))
tables_orm = oem2orm.collect_tables_from_oem(db, metadata_folder)
# %%
