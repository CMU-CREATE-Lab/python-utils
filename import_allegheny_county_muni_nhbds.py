import geopandas as gpd
import epsql

engine = epsql.Engine()

gdf = gpd.read_file("/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/geojson/AllegCo_Munis_PghNeighs_cap_2d_split.geojson")
epsql.sanitize_column_names(gdf, inplace=True)
gdf.rename_geometry('geom', inplace=True)
gdf.to_crs(epsg=4326, inplace = True)
table_name = 'allegheny_county_muni_nhbds'
# Write to database table
with engine.connect() as con:
    gdf.to_postgis(table_name, con, if_exists='replace')
engine.repair_geometries_if_needed(table_name)
