
#%%

import geopandas as gpd
import utils
import epsql

engine = epsql.Engine()

#%%

schema = 'pasda_wgs84'

def import_pasda_parcels(name, force=False):
    table_name = epsql.sanitize_column_name(name)
    if engine.table_exists(f'{schema}.{table_name}') and not force:
        print(f'Table {schema}.{table_name} already exists, skipping')
        return

    # Download and extract shapefile
    dir = 'pasda_downloads'
    utils.download_file(f'https://www.pasda.psu.edu/download/alleghenycounty/{name}.zip', f'{dir}/{name}.zip')
    utils.unzip_file(f'{dir}/{name}')
    shapefile = f'{dir}/{name}/{name}.shp'

    # Read and reproject shapefile
    gdf = gpd.read_file(shapefile)
    # pin isn't unique, so copy df integer index to a column to become primary key
    gdf.insert(0, 'idx', gdf.index)
    print(f'Read {len(gdf)} parcels from {shapefile}')
    epsql.sanitize_column_names(gdf, inplace=True)
    gdf.rename_geometry('geom', inplace=True)
    gdf.to_crs(epsg=4326, inplace = True)

    # Write to database table
    engine.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    with engine.connect() as con:
        gdf.to_postgis(table_name, con, schema = schema, if_exists='replace')
    engine.execute(f"ALTER TABLE {schema}.{table_name} ADD PRIMARY KEY (idx)")
    engine.execute(f"CREATE INDEX {table_name}_pin_idx ON {schema}.{table_name} (pin);")

    print(f'Wrote {len(gdf)} records to {schema}.{table_name}')

    engine.repair_geometries_if_needed(f'{schema}.{table_name}')
    engine.add_highest_overlap_crosswalk(f'{schema}.{table_name}', 'block_2010', 'tiger_wgs84.tl_2010_tabblock10', 'geoid10')


import_pasda_parcels('AlleghenyCounty_Parcels202107', True)


# %%
