#%%

from import_tiger import *

# Be sure to add info to https://docs.google.com/spreadsheets/d/1nyF_53OEzQvZvmbfp_WFNlKS2gWipGyDTK86WaleGmE/edit#gid=0

for year in [2010, 2019]:
    for level in tiger_levels(year):
        load_tiger_geometries(year, all_state_fips, level=level, drop_first=False)

# %%
