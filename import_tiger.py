#%%

import glob, os, psycopg2, re, sys
import geopandas as gpd

# class ScriptDirInPath:
#     def __enter__(self): sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
#     def __exit__(self, *_): sys.path.remove(os.path.dirname(os.path.realpath(__file__)))

#with ScriptDirInPath():
from utils import utils
from utils import epsql

#%%
tiger_downloads = 'tiger_downloads'

class TigerLevel:
    level_name: str
    download_subdir: bool
    geoid_column_name: str
    download_by_state: bool
    def __init__(self, level_name, download_subdir=None, download_by_state=True):
        self.level_name = level_name
        if download_subdir == None:
            download_subdir = level_name.upper()
        self.download_subdir = download_subdir
        if level_name == 'tabblock10':
            self.geoid_column_name = 'geoid10'
        elif level_name == 'tabblock20':
            self.geoid_column_name = 'geoid20'
        else:
            self.geoid_column_name = 'geoid'
        self.download_by_state = download_by_state

def tiger_levels(year):
    # county = TigerLevel('county', download_subdir="COUNTY", download_by_state=False)
    # tract = TigerLevel("tract")
    # blockgroup = TigerLevel("bg")
    # #decade = int((year % 100) / 10) * 10
    # decade = 20
    # block = TigerLevel(f"tabblock{decade}")
    # return [county, tract, blockgroup, block]

    if year == 2019 or year == 2018:
        return [
            TigerLevel('tract'), 
            TigerLevel('bg'), 
        ]
    elif year == 2010:
        tract = TigerLevel("tract10")
        # return [
        #     TigerLevel('tract10'),
        #     TigerLevel('bg10'),
        #     TigerLevel('tabblock10')]
    elif year == 2020:
        return [
            TigerLevel('tract'),
            TigerLevel('bg'),
            TigerLevel('tabblock20'),
            TigerLevel('tabblock10', download_subdir="TABBLOCK"),
            TigerLevel('county', download_subdir="COUNTY", download_by_state=False),
        ]
    else:
        assert(False)
    
def tiger_name(year: int, state_fips: str, level: TigerLevel):
    return f'tl_{year}_{state_fips}_{level.level_name}'
    
def tiger_table_name(year: int, level: TigerLevel):
    return f'tiger_wgs84.tl_{year}_{level.level_name}'

# def tiger_geoid(year: int, level: TigerLevel):
#     if level == 'tabblock10':
#         return 'geoid10'
#     else:
#         return 'geoid'

# def tiger_level_download_dir(level):
#     # Uppercase and remove digits
#     return re.sub(r"\d", "", level.upper())

def tiger_reference_year(level):
    yy = level[-2:]
    assert(yy == '10' or yy == '20')
    return '20' + yy

def tiger_src(year: int, state_fips: str, level: TigerLevel):
    name = tiger_name(year, state_fips, level)
    src = f'https://www2.census.gov/geo/tiger/TIGER{year}/{level.download_subdir}/'
    if year == 2010:
        src += f'{tiger_reference_year(level.level_name)}/'
    src += f'{name}.zip'
    return src


# TIGER2010/TRACT/2010/tl_2010_01_tract10.zip
# https://www2.census.gov/geo/tiger/TIGER2010/BG/2010/tl_2010_01_bg10.zip
# https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_01_tabblock10.zip

def get_tiger_shapefile_path(year, state_fips, level):
    name = tiger_name(year, state_fips, level)
    dest = f'{tiger_downloads}/{name}'

    if not os.path.exists(dest):
        src = tiger_src(year, state_fips, level)

        utils.download_file(src, f'{dest}.zip')
        utils.unzip_file(f'{dest}.zip')
        os.unlink(f'{dest}.zip')
    
    shapefile_candidates = glob.glob(f'{dest}/*.shp')
    assert len(shapefile_candidates) == 1
    return shapefile_candidates[0]

def read_tiger_shapefile_as_wgs84(year: int, state_fips: str, level: TigerLevel):
    gdf = gpd.read_file(get_tiger_shapefile_path(year, state_fips, level))
    epsql.sanitize_column_names(gdf, inplace=True)
    gdf.rename_geometry('geom', inplace=True)
    gdf.to_crs(epsg=4326, inplace = True)
    return gdf


# %%

def index_tiger_geometries(engine, table_name:str, level:TigerLevel):
    schema = epsql.get_schema(table_name)
    with engine.connect() as con:
        con.execute(f'CREATE INDEX IF NOT EXISTS {epsql.get_table_name(table_name)}_geom_idx ON {table_name} USING GIST (geom)')
        con.execute(f'CREATE INDEX IF NOT EXISTS {epsql.get_table_name(table_name)}_geoid_idx ON {table_name} ({level.geoid_column_name})')

def load_tiger_geometries(engine, year: int, level:TigerLevel , drop_first=False):
    table_name = tiger_table_name(year, level)
    schema = epsql.get_schema(table_name)
    if schema != 'public':
        engine.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    if drop_first:
        engine.execute(f'DROP TABLE IF EXISTS {table_name}')
    if level.level_name.endswith('10') or level.level_name.endswith('20'):
        suffix=level.level_name[-2:]
    else:
        suffix=''
    with engine.connect() as con:
        already_loaded = []
        if level.download_by_state:
            fips_list = all_state_fips
        else:
            fips_list = ['us']
        for fips in fips_list:
            if level.download_by_state:
                state_filter = f"WHERE {level.geoid_column_name} LIKE '{fips}%%'"
            else:
                state_filter = ''
            if con.table_exists(table_name) and con.execute_exists(f"SELECT EXISTS (SELECT * FROM {table_name} {state_filter} LIMIT 1)"):
                already_loaded.append(fips)
            else:
                gdf = read_tiger_shapefile_as_wgs84(year, fips, level)
                gdf.to_postgis(
                    epsql.get_table_name(table_name),
                    con, 
                    schema = schema,
                    if_exists='append')
                print(f'{table_name}: Added {len(gdf)} records from FIPS {fips}')
                index_tiger_geometries(engine, table_name, level)
        if already_loaded:
            print(f'{table_name}: Already loaded FIPS {", ".join(already_loaded)}')
        index_tiger_geometries(engine, table_name, level)

all_state_fips = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78"
]



#%%

def add_census_geoids(engine, dest_table, dest_geom_column, year, verbose=False):
    print(f'Adding census geoids to {dest_table}.{dest_geom_column} from TIGER year {year}')
    for level in tiger_levels(year):
        census_table = tiger_table_name(year, level)
        census_geoid_column = level.geoid_column_name
        dest_geoid_column = f'{dest_geom_column}_{level}_{census_geoid_column}'
        engine.execute(f'ALTER TABLE {dest_table} DROP COLUMN IF EXISTS {dest_geoid_column}')
        engine.execute(f'ALTER TABLE {dest_table} ADD COLUMN {dest_geoid_column} TEXT')
        cmd = f"""
            UPDATE {dest_table} AS dest
            SET {dest_geoid_column} = tiger.{census_geoid_column}
            FROM {census_table} AS tiger
            WHERE ST_Contains(tiger.geom, dest.{dest_geom_column})"""
        engine.execute(cmd, verbose=verbose)
        geoid_count = engine.execute_count(f'SELECT COUNT({dest_geoid_column}) FROM {dest_table}')
        all_count = engine.execute_count(f'SELECT COUNT(*) FROM {dest_table}')
        print(f'  Created {dest_table}.{dest_geoid_column}, finding {geoid_count} of {all_count} records')
# %%
