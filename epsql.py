
#%%

"""epsql:  Extensions to SQLAlchemy engine and connection

Includes TIGER geocoding, simplified UNIX socket connection, convenience wrappers for execute"""


import functools, os, re, sqlalchemy, threading, time, types

def sanitize_column_name(colname: str):
    colname = re.sub(r'^\W+', '', colname) # Remove leading non-word-chars
    colname = re.sub(r'\W+$', '', colname) # Remove trailing non-word-chars
    colname = re.sub(r'\W+', '_', colname) # Replace contiguous sets of non-word-chars with underscore
    return colname.lower()

def sanitize_column_names(df, inplace=False):
    return df.rename(columns={c:sanitize_column_name(c) for c in df.columns}, inplace=inplace)

# Returns table name portion of table_name_with_optional_schema
def get_table_name(table_name_with_optional_schema):
    if '.' in table_name_with_optional_schema:
        return table_name_with_optional_schema.split('.')[-1]
    else:
        return table_name_with_optional_schema

# Returns schema name portion of table_name_with_optional_schema;  'public' if no schema specified
def get_schema(table_name_with_optional_schema):
    if '.' in table_name_with_optional_schema:
        return table_name_with_optional_schema.split('.')[0]
    else:
        return 'public'

class ConnectionExtensions(sqlalchemy.engine.base.Connection):
    """Extensions for Connection and Engine

    This class is never instantiated directly, but rather patched in upon construction
    of Engine and Connection objects"""

    def __init__(self):
        raise Exception(
            "Don't instantiate this class directly;  using epsql.Engine will patch these methods "
            "into Connection and Engine instances.")

    def execute(self, *args, verbose=False, **kwargs):
        if verbose:
            print(f'{args[0]}')
        return sqlalchemy.engine.base.Connection.execute(self, *args, **kwargs)

    def execute_returning_dicts(self, *args, **kwargs):
        results = self.execute(*args, **kwargs)
        return [dict(rec) for rec in results]

    # For kwargs, see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
    def execute_returning_df(self, sql, **kwargs):
        import pandas as pd
        return pd.read_sql_query(sql, self, **kwargs)

    # For kwargs, see https://geopandas.org/reference/geopandas.read_postgis.html#geopandas.read_postgis
    def execute_returning_gdf(self, sql, **kwargs):
        import geopandas as gpd
        return gpd.read_postgis(sql, self, **kwargs)

    #def df_to_table(self, df, table_name, **kwargs):
    #    with Stopwatch(f'Adding {len(df)} records to {table_name}'):
    #        sanitize_column_names(df).to_sql(table_name, self, **kwargs)

    def table_exists(self, table_name):
        return self.execute_exists(f"""SELECT EXISTS (
            SELECT FROM pg_tables WHERE schemaname='{get_schema(table_name)}' AND tablename='{get_table_name(table_name)}')""")

    def execute_exists(self, sql, **kwargs):
        return self.execute_returning_dicts(sql, **kwargs)[0]['exists']

    def execute_count(self, sql, **kwargs):
        return self.execute_returning_dicts(sql, **kwargs)[0]['count']

    def geocode(self, address, max_results=1, latlon_only=False):
        if latlon_only:
            sel = """
            SELECT rating, geomout, to_jsonb((addy)) as addy
            FROM geocode(pagc_normalize_address(%(address)s), %(max_results)s) As g;"""
        else:
            sel = """
                SELECT rating, 
                       geomout, 
                       to_jsonb((addy)) as addy,
                       to_jsonb(tabblock.*) - 'the_geom' as block,
                       to_jsonb(bg.*) - 'the_geom' as blockgroup,
                       to_jsonb(tract.*) - 'the_geom' as tract
                FROM geocode(pagc_normalize_address(%(address)s), %(max_results)s) As g
                LEFT JOIN tabblock ON ST_Contains(tabblock.the_geom, geomout)
                LEFT JOIN bg ON ST_Contains(bg.the_geom, geomout)
                LEFT JOIN tract ON ST_Contains(tract.the_geom, geomout)
                """
        return self.execute_returning_dicts(sel, address=address, max_results=max_results)

def _with_connect(engine, member_name, *args, **kwargs):
    with engine.connect() as con:
        return getattr(con, member_name)(*args, **kwargs)

def _find_pghost():
    candidates = [
        "/host-postgresql", # talk to host's postgresql from docker (cocalc)
        "/var/run/postgresql"  # Ubuntu default location
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise Exception(f'Attempting to find unix socket for postgresql, but cannot find any of {candidates}')

class Engine(ConnectionExtensions):

    def __init__(self, engine=None, db_name="earthtime"):
        if not engine:
            # cocalc sets PGUSER to something unhelpful
            if 'PGUSER' in os.environ:
                del os.environ['PGUSER']

            # There's no syntax available for embedding a socket directory path in the postgresql:/// url,
            # so we need to set instead using environment variable
            os.environ["PGHOST"] = _find_pghost()

            print(f'Connecting to database {db_name} with host={os.environ["PGHOST"]}')            
            engine = sqlalchemy.create_engine(
               f'postgresql:///{db_name}',
               connect_args={"options": "-c timezone=utc"})

        # Patch engine with connection-oriented extensions from ConnectionExtensions
        for member_name, member in ConnectionExtensions.__dict__.items():
            if isinstance(member, types.FunctionType) and member_name[:2] != "__":
                setattr(self, member_name, functools.partial(_with_connect, self, member_name))

        self.engine = engine

    def connect(self) -> ConnectionExtensions:
        con = self.engine.connect()

        # Patch connection with extensions from ConnectionExtensions
        for member_name, member in ConnectionExtensions.__dict__.items():
            if isinstance(member, types.FunctionType) and member_name[:2] != "__":
                setattr(con, member_name, types.MethodType(member, con))

        return con
    
    def geocode_batch(self, addresses, max_results=1, nthreads=10):
        mutex = threading.Lock()
        ret = [None] * len(addresses)
        i = len(addresses)
        def geocode():
            with self.connect() as con:
                nonlocal i
                while True:
                    with mutex:
                        if i == 0:
                            return
                        i -= 1
                        mine = i
                    ret[mine] = con.geocode(addresses[mine], max_results)
        try:
            threads = [ThCall(geocode) for x in range(nthreads)]
            for thread in threads:
                thread.join()
        finally:
            i = 0
            for thread in threads:
                thread.join()
        return ret

# #%%
# engine.geocode('3634 Frazier St, Pittsburgh PA')
# #%%

# type(results[0].items()[0][1])
# #%%
# df = engine.execute_returning_df('SELECT * FROM pa_act91_table LIMIT 2')
# df
# #%%
# engine.execute_returning_dicts("""SELECT * FROM geocode(%(address)s, 1) As g;""", address="3634 Frazier St, Pittsburgh PA 15213")
# #%%
# engine.execute_returning_dicts("""
# SELECT * FROM 
#     (VALUES (%(index)s, %(input_address)s)) As a(index, input_address)
# LEFT JOIN LATERAL
#     geocode(a.input_address, 1) As g
# ON true""",
#     index=1, input_address="3634 Frazier St, Pittsburgh PA 15213")

# #%%
# engine.execute_returning_dicts("""
# SELECT * FROM 
#     (VALUES (%(index)s, %(input_address)s)) As a(index, input_address)
# LEFT JOIN LATERAL
#     geocode(a.input_address, 1) As g
# ON true""",
#     [{'index':1, 'input_address':"3634 Frazier St, Pittsburgh PA 15213"},
#     {'index':1, 'input_address':"3634 Frazier St, Pittsburgh PA 15213"}])
# #%%
# engine.execute_returning_dicts("""
#          SELECT geocode('3634 Frazier St, Pittsburgh PA 15213', 1)""")
# #%%
# engine.execute_returning_dicts("""
#          SELECT a.index, a.input_address, g.addy, g.geomout, g.rating FROM
#              (VALUES (%(index)s, %(input_address)s)) As a(index, input_address)
#          LEFT JOIN LATERAL
#              geocode(a.input_address, 1) As g
#          ON true""",
#          index=1, input_address='3634 Frazier St, Pittsburgh PA 15213')
# #%%


# #     execute_sql_gdf("""
# #         SELECT a.index, a.input_address, g.addy, g.geomout, g.rating FROM
# #             (SELECT index, input_address FROM pa_act91_table WHERE rating IS NULL LIMIT 100) As a
# #         LEFT JOIN LATERAL
# #             geocode(a.input_address, 1) As g
# #         ON true""",
# #         geom_col='geomout',
# #         index_col='index')


# #%%
# def psql_type_from_column(col):
#     import pandas as pd
#     # Adapted from pandas.io.sql.py

#     # Infer type of column, while ignoring missing values
#     # Needed for inserting typed data containing NULLs, GH 8778.
#     col_type = pd._libs.lib.infer_dtype(col, skipna=True)

#     if col_type == "datetime64" or col_type == "datetime":
#         # GH 9086: TIMESTAMP is the suggested type if the column contains
#         # timezone information
#         try:
#             if col.dt.tz is not None:
#                 return TIMESTAMP(timezone=True)
#         except AttributeError:
#             # The column is actually a DatetimeIndex
#             # GH 26761 or an Index with date-like data e.g. 9999-01-01
#             if getattr(col, "tz", None) is not None:
#                 return "timestamp with time zone"
#         return "timestamp without time zone"
#     if col_type == "timedelta64":
#         print("the 'timedelta' type is not supported, and will be "
#               "written as integer values (ns frequency) to the database.")
#         return "int8"
#     elif col_type == "floating":
#         if col.dtype == "float32":
#             return "float4"
#         else:
#             return "float8"
#     elif col_type == "integer":
#         # GH35076 Map pandas integer to optimal SQLAlchemy integer type
#         if col.dtype.name.lower() in ("int8", "uint8", "int16", "uint16", "int32"):
#             return "int4"
#         elif col.dtype.name.lower() == "uint64":
#             raise ValueError("Unsigned 64 bit integer datatype is not supported")
#         else:
#             return "int8"
#     elif col_type == "boolean":
#         return "boolean"
#     elif col_type == "date":
#         return "date"
#     elif col_type == "time":
#         return "time"
#     elif col_type == "complex":
#         raise ValueError("Complex datatypes not supported")
#     else:
#         return "text"

# #%%
# df = engine.execute_returning_df('SELECT * FROM pa_act91_table WHERE rating IS NULL LIMIT 2')
# # The real thing we want is upserting from to_sql
# # https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update

# def inline_df(df, as_table_name):
#     params = []
#     for col in df.columns:
#         params.append(f'{sanitize_column_name(col)} {psql_type_from_column(df[col])}')
#     return f"json_to_recordset(%({as_table_name}_data)s) as {as_table_name}({', '.join(params)})"

# inline_df(df, 'foo')

# #%%



# #%%

# with engine.connect() as con:
#     con.execute(inline_df())
# """, params=dict(records=json.dumps([{"index":1,"geom":"abc"},{"index":2,"geom":"def"}]))))


# execute_sql_df("""
# SELECT * FROM 


# # def geocode(addresses_df):
    
# #     execute_sql_gdf("""
# #         SELECT a.index, a.input_address, g.addy, g.geomout, g.rating FROM
# #             (SELECT index, input_address FROM pa_act91_table WHERE rating IS NULL LIMIT 100) As a
# #         LEFT JOIN LATERAL
# #             geocode(a.input_address, 1) As g
# #         ON true""",
# #         geom_col='geomout',
# #         index_col='index')




#     # execute_sql_gdf("""
#     #     SELECT a.index, a.input_address, g.addy, g.geomout, g.rating FROM
#     #         (SELECT index, input_address FROM pa_act91_table WHERE rating IS NULL LIMIT 100) As a
#     #     LEFT JOIN LATERAL
#     #         geocode(a.input_address, 1) As g
#     #     ON true""",
#     #     geom_col='geomout',
#     #     index_col='index')



# # %%

# %%
