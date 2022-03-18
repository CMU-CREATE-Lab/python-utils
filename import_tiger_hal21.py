#%%

import import_tiger
import epsql
engine = epsql.Engine()

# Be sure to add info to https://docs.google.com/spreadsheets/d/1nyF_53OEzQvZvmbfp_WFNlKS2gWipGyDTK86WaleGmE/edit#gid=0

for year in [
#    2010,
#    2018,
#    2019,
    2020
    ]:
    for level in import_tiger.tiger_levels(year):
        import_tiger.load_tiger_geometries(
            engine,
            year,
            import_tiger.all_state_fips,
            level=level,
            drop_first=False)

print("Done")
# %%
