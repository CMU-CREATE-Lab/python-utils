
#%%

import geopandas as gpd
import utils
import epsql

engine = epsql.Engine()

#%%

schema = 'pasda_wgs84'
def import_pasda_parcels(name):
    table_name = epsql.sanitize_column_name(name)
    if engine.table_exists(f'{schema}.{table_name}'):
        print(f'Table {schema}.{table_name} already exists, skipping')
        return
    dir = 'pasda_downloads'
    utils.download_file(f'https://www.pasda.psu.edu/download/alleghenycounty/{name}.zip', f'{dir}/{name}.zip')
    utils.unzip_file(f'{dir}/{name}')
    shapefile = f'{dir}/{name}/{name}.shp'
    gdf = gpd.read_file(shapefile)
    print(f'Read {len(gdf)} parcels from {shapefile}')
    epsql.sanitize_column_names(gdf, inplace=True)
    gdf.rename_geometry('geom', inplace=True)
    gdf.to_crs(epsg=4326, inplace = True)

    engine.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    with engine.connect() as con:
        gdf.to_postgis(table_name, con, schema = schema, if_exists='replace')
    engine.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {schema}.{table_name} USING GIST (geom);")
    engine.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_pin_idx ON {schema}.{table_name} (pin);")
    print(f'Wrote {len(gdf)} records to {schema}.{table_name}')

import_pasda_parcels('AlleghenyCounty_Parcels202107')
