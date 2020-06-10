import datetime, io, psycopg2, re, time
import numpy as np
import pandas as pd

class Psql:
    class Stopwatch:
        def __init__(self, db, name):
            self.name = name
            self.db = db
        def __enter__(self):
            self.start = time.time()
        def __exit__(self, type, value, traceback):
            self.db.info(f'{self.name} took {time.time() - self.start:1f} seconds')

    class Transaction:
        def __init__(self, db, suppress_errors=False):
            self.db = db
            self.suppress_errors = suppress_errors
        
        def __enter__(self):
            if self.db._transaction_count == 0:
                #self.db.info('Starting transaction')
                pass
            self.db._transaction_count += 1
            if not self.db._cur:
                self.db._cur = self.db._con.cursor()
            return self.db._cur
        
        def __exit__(self, type, value, tb):
            self.db._transaction_count -= 1
            if self.db._transaction_count == 0:
                if tb is None:
                    #self.db.info('Committing transaction')
                    self.db._con.commit()
                else:
                    if not self.suppress_errors:
                        self.db.error('Exception; rolling back transaction')
                    self.db._con.rollback()
                self.db._cur.close()
                self.db._cur = None

    def __init__(self, dbname=None, user=None, password=None, host=None, port=None, **connect_kwargs):
        for kwarg in ('dbname', 'user', 'password', 'host', 'port'):
            if locals()[kwarg] != None:
                connect_kwargs[kwarg] = locals()[kwarg]
        self._con = psycopg2.connect(**connect_kwargs)
        self._cur = None # get cursors using with db.tranaction() as cursor
        self._transaction_count = 0
    
    def transaction(self, suppress_errors=False):
        return self.Transaction(self, suppress_errors=suppress_errors)

    def execute(self, query, vars=None, verbose=False, suppress_errors=False):
        with self.transaction(suppress_errors=suppress_errors) as cursor:
            if verbose:
                self.info(f'execute {query}')
            return cursor.execute(query, vars)

    def copy_expert(self, sql, file, *args, **kwargs):
        with self.transaction() as cursor:
            self.info(f'copy_expert {sql}')
            return cursor.copy_expert(sql, file, *args, **kwargs)
    
    def info(self, *args, **kwargs):
        print('Psql:', *args, **kwargs)

    def error(self, *args, **kwargs):
        print('Psql error:', *args, **kwargs)

    def create_empty_table_from_df(self, table_name, df, override_types={}, primary_key=None, dry_run=False):
        type_map = {
            np.dtype('O'): 'text',
            np.dtype('float64'): 'float8',
            np.dtype('int64'): 'int8',
            np.dtype('bool'): 'bool',
            np.dtype('datetime64[ns]'): 'date'   # date without time or timezone.  use override_types for other choices
        }

        def col_constraint(col):
            if col == primary_key:
                return " PRIMARY KEY"
            else:
                return ""

        def col_type(col):
            if col in override_types:
                return override_types[col]
            try:
                # If geopandas is loaded, look for GeometryDtype
                if isinstance(df[col].dtype, gpd.array.GeometryDtype):
                    return 'geometry'
            except:
                pass
            return type_map[df[col].dtype]

        sql_cols = [f"    {self.sanitize_column_name(col):63s} {col_type(col)}{col_constraint(col)}" for col in df.columns]
        sql_cols = ',\n'.join(sql_cols)

        cmd = f"CREATE TABLE {table_name} (\n{sql_cols}\n)"

        if dry_run:
            print(cmd)
        else:
            self.execute(cmd)

    def stopwatch(self, name):
        return self.Stopwatch(self, name)

    def append_df_to_table(self, df, table_name):
        col_names = [self.sanitize_column_name(c) for c in df.columns]
        with self.stopwatch(f'Creating csv of {len(df)} records for {table_name}'):
            csv = io.StringIO(df.to_csv(index=False))
        with self.stopwatch(f'Appending csv of {len(df)} records to {table_name}'):
            # postgres ignores CSV header!  so be sure we specify the column names correctly
            self.copy_expert(sql=f"COPY {table_name} ({','.join(col_names)}) FROM stdin DELIMITER ',' CSV header;",
                            file=csv)
        self.info(f'Wrote {len(df)} records to {table_name}')

    def select_as_df(self, cmd, args=()):
        return pd.DataFrame(self.select_records(cmd, args, parse_geo=False))
    
    def select_as_gdf(self, cmd, args=()):
        return gpd.GeoDataFrame(self.select_records(cmd, args, parse_geo=True))

    # TO DO: convert date types into python dates
    def select_records(self, cmd, args=(), parse_geo=True):
        geometry_type_code = None
        if parse_geo and 'gpd' in vars():
            # If geopandas is loaded as gpd, try to find type_code for postgis geometry type
            # TODO: don't barf if it's not a postgresql
            print("TODO: test me!")
            rec = self.select_record_or_none("SELECT oid FROM pg_catalog.pg_type WHERE typname='geometry';")
            if rec:
                geometry_type_code = rec['oid']

        with self.transaction() as cursor:
            self.execute(cmd, args)
            col_names = [col.name for col in cursor.description]
            records = cursor.fetchall()

            if geometry_type_code and len(records):
                # For each geometry column...
                for i in range(len(col_names)):
                    if cursor.description[i].type_code == geometry_type_code:
                        # If records are tuples, convert to lists to be mutable
                        if isinstance(records[0], tuple):
                            records = [list(record) for record in records]
                        print(f'Converting {col_names[i]} (column {i}) to geometry')
                        # Convert from hex to object
                        for record in records:
                            record[i] = shapely.wkb.loads(record[i], hex=True) if not pd.isna(record[i]) else np.nan

        ret = []
        for record in records:
            ret.append(dict(zip(col_names, record)))
        return ret

    def select_record(self, cmd, args=()):
        records = self.select_records(cmd, args)
        if len(records) != 1:
            raise Exception(f'Expected 1 record but received {len(records)}')
        return records[0]

    def select_record_or_none(self, cmd, args=()):
        records = self.select_records(cmd, args)
        if not records:
            return None
        if len(records) != 1:
            raise Exception(f'Expected 0 or 1 record but received {len(records)}')
        return records[0]

    def insert_record(self, table, dic):
        keys = ','.join(dic.keys())
        values = ','.join(['%s'] * len(dic))
        cmd = f"INSERT INTO {table} ({keys}) VALUES ({values})"
        self.execute(cmd, tuple(dic.values()))

    # Sanitize column names for sql by:
    # - removing leading and trailing non-word characters
    # - changing internal sequences of non-word chars to a single underscore
    # - lowercasing
    def sanitize_column_name(self, colname):
        colname = re.sub('\W+$', '', colname)
        colname = re.sub('^\W+', '', colname)
        colname = re.sub('\W+', '_', colname)
        return colname.lower()
