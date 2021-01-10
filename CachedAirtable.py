#%%
# CachedAirtable.py

# Hybridized from resourcenav's airtable_cache.py and court_scrapers' court_scrapers_psql.ipynb
# by Anne Wright on 1/9/2021

import datetime, dateutil.parser, json, os, pprint, pytz, re, requests, sqlalchemy
pp = pprint.PrettyPrinter(indent=2).pprint
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, inspect, select, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import DateTime, TIMESTAMP, TypeDecorator
from sqlalchemy.dialects.postgresql import insert as psql_insert
import time

class UTCDateTime(TypeDecorator):
    impl = DateTime(timezone=True)

    def process_bind_param(self, value, dialect):
        if value is not None:
            if type(value) == str:
                value = dateutil.parser.isoparse(value)
            if not value.tzinfo:
                raise TypeError("tzinfo is required")
            value = value.astimezone(pytz.utc)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = value.astimezone(pytz.utc)
        return value

'utils' in vars() and vars()['utils'].reload_module('utils.utils')
import utils.utils as utils
from collections import defaultdict

# Load the support for accessing Airtable.  Make sure to instantiate a 
# CachedAirtableEngine to set up airtable_config_map
utils.reload_module('utils.AirtableTools')
from utils.AirtableTools import *

#############################################
# Define SqlAlchemy classes to represent the tables
# for the Airtable postgres mirroring.  These
# transcend which engine would connect to a server
metadata = MetaData()

#############################################
# multiple primary_key=True gives us a primary composite key
mirror_table = Table('airtable_mirror', metadata,
    Column('baseKey', String, primary_key=True),
    Column('tablename', String, primary_key=True),
    Column('view', String, primary_key=True),
    Column('id', String, primary_key=True),     
    Column('mirroredTime', UTCDateTime),
    Column('createdTime', UTCDateTime),
    Column('fields', JSONB))

# multiple primary_key=True gives us a primary composite key
metadata_table = Table('airtable_mirror_metadata', metadata,
    Column('baseKey', String, primary_key=True),
    Column('tablename', String, primary_key=True),
    Column('view', String, primary_key=True),
    Column('mirroredTime', UTCDateTime))


# A CachedAirtableEngine wraps a SqlAlchemy engine 
# object, meaning a connection to a specific server
# These used to be globals in the parent files that 
# this module grew from, but they needed to be 
# encapsulated to allow for connecting to multiple 
# psql instances

# It's required to pass in an airtable_config_map and db_name.

# Postgres is in different places on different systems.  If 
# your system has it somewhere other than /var/run/postgresql/ 
# then set up engine and pass it in to CachedAirtableEngine

# If no engine is passed in then the it will use a default
# of /var/run/postgresql/
class CachedAirtableEngine:
    def __init__(self, airtable_config_map, engine=None, db_name=None, psql_user=None,drop_tables=False):
        # Initialize AirtableWrapper.  This is defined in utils.AirtableTools and requires a key be in airtable_key.txt
        self.airtable_config_map = airtable_config_map
        self.aw = AirtableWrapper(airtable_config_map)

        if not engine:
            print(f"Using default postgresql setup from /var/run/postgresql/")
            print(f"If using on a different configuration, pass engine")
            print(f"into CachedAirtableEngine constructor from CachedAirtable.py")

            assert db_name and psql_user, "ERROR: If not passing engine in to CachedAirtableEngine, must set db_name and psql_user"

            # Override host to /var/run/postgresql/ for Ubuntu 18.04 native postgresql
            os.environ["PGHOST"] = "/var/run/postgresql/"
            engine = sqlalchemy.create_engine(
                f'postgresql:///{db_name}', 
                connect_args={"options": "-c timezone=utc"}
                )

        self.engine = engine

        ############################################
        if drop_tables:
            print("  Dropping airtable_mirror and airtable_mirror_metadata")
            with engine.connect() as con:
                rs = con.execute('DROP TABLE IF EXISTS airtable_mirror;')
                print('Dropped airtable_mirror')
                rs = con.execute('DROP TABLE IF EXISTS airtable_mirror_metadata;')
                print('Dropped airtable_mirror_metadata')

        ############################################
        missing = set(metadata.tables.keys()) - set(engine.table_names())
        if missing:
            print(f'Creating [{", ".join(missing)}]')
            engine.echo = True
            metadata.create_all(engine)
            engine.echo = False
        else:
            print(f'Tables [{", ".join(metadata.tables.keys())}] already exist')

        ###########################################
        # Set permissions
        try:
            psql_user
        except:
            psql_user = "www-data"

        print(f"  Giving table permissions to psql_user '{psql_user}'")
        with engine.connect() as con:
            for table in metadata.tables.keys():
                con.execute(f'GRANT ALL ON {table} TO "{psql_user}"')

    def connect(self):
        return self.engine.connect()

    def get_aw(self):
        return self.aw

    def get_airtable_config_map(self):
        return self.airtable_config_map


# %%
# cat_engine is CachedAirtableEngine.  This encapsulates the 
# SqlAlchemy engine, AirtableWrapper, and airtable_config_map
class CachedAirtableView:
    def __init__(self, baseName, tablename, view, key_col=None, cat_engine=None):
        self.baseName = baseName
        self.tablename = tablename
        assert cat_engine, "ERROR: Must provide cat_engine arg to create a CachedAirtableView"
        self.engine = cat_engine

        self.table = cat_engine.get_aw().get_table(baseName, tablename)
        self.baseKey = self.table.url_table.split('/')[-2]
        self.view = view
        self.key_col = key_col

        # key_map is for records which are currently active in the airtable
        # and has one entry per key
        self.key_map = {}

    def compositeName(self):
        return f'{self.baseName}:{self.tablename}:{self.view}'

    def db_indices(self):
        return dict(baseKey=self.baseKey, tablename=self.tablename, view=self.view)

    def __repr__(self):
        return f'<CachedAirtableView {self.compositeName()}>'

    # Will mutate records by setting mirroredTime, baseKey, tablename, view
    def upsert(self, con, records, mirroredTime):
        if not records:
            return
        for record in records:
            record.update(self.db_indices()) # set baseKey, tablename, view
            record['mirroredTime'] = mirroredTime

        before = con.scalar(mirror_table.count())
        ins = psql_insert(mirror_table)
        upsert = ins.on_conflict_do_update(
            constraint=mirror_table.primary_key,
            set_=dict(
                mirroredTime=ins.excluded.mirroredTime,
                createdTime=ins.excluded.createdTime,
                fields=ins.excluded.fields))
        con.execute(upsert, records)
        after = con.scalar(mirror_table.count())
        n_inserted = after - before
        print(f'Upserted {len(records)} records ({len(records)-n_inserted} updated, {n_inserted} inserted): {self.compositeName()}')
    
        
    # Do write-through of item.  If key_col is set, check if this is a dup and do update if so.
    # If not dup, keep track of new key in key_map
    def insert(self, ins_obj):
        key = ins_obj[self.key_col] if self.key_col in ins_obj else None
        
        if key and key in self.key_map:
            rec_id = self.key_map[key]
            print(f"Already have key {key}={rec_id} in {self.tablename}: {ins_obj}, switching to update")
            
            return self.update(rec_id,ins_obj)
            
        print(f"Inserting into airtable {self.tablename}: {ins_obj}, key = '{key}'")
        mirror_date = datetime.datetime.now(datetime.timezone.utc)
        ins_res = self.table.insert(ins_obj)
        
        assert not ins_res is None and 'id' in ins_res, f"  {self.tablename}: Failed to add {key}:{ins_res}, {ins_obj}"
        if key:
            self.key_map[key] = ins_res['id']
            
        print("  Adding to postgres")
        with self.engine.connect() as con:
            self.upsert(con,[ins_res],mirror_date)
            
        return ins_res
    
    # Do write-through of item
    def update(self,up_id, up_obj):
        print(f"Updating airtable {self.tablename}: {up_id} -> {up_obj}")
        mirror_date = datetime.datetime.now(datetime.timezone.utc)
        up_res = self.table.update(up_id,up_obj)
        
        assert not up_res is None and 'id' in up_res, f"  {self.tablename}: Failed to update {up_id}:{up_res}, {up_obj}"
            
        print("  Adding to postgres")
        with self.engine.connect() as con:
            self.upsert(con,[up_res],mirror_date)
            
        return up_res
    
    def get_count(self):
        return len(self.key_map)
    
    def get_key_map(self):
        return self.key_map 
        
    def get_all_keys(self):
        return list(self.key_map.keys())
    
    # Get a a mirrored entry in this table by ID.  
    # refresh_if_older is a datetime.timedelta OR an absolute datetime
    # If there is an entry in postgres that's sufficiently fresh that will 
    # be returned.  Otherwise there will be an airtable request made.
    def get(self, rec_id, refresh_if_older=None):
        ret_rec = None
        with self.engine.connect() as con:
            ret_rec = self.find_one_or_none(con, id=rec_id, refresh_if_older=refresh_if_older)
            
        # Check if there is a key
        if (ret_rec and 'id' in ret_rec and 
            'fields' in ret_rec and self.key_col in ret_rec['fields']):
            key = ret_rec['fields'][self.key_col] 
            self.key_map[key] = ret_rec['id']
        
        return ret_rec
    
    def get_by_key(self, key, refresh_if_older=None):
        if key in self.key_map:
            rec_id = self.key_map[key]
            return self.get(rec_id, refresh_if_older=refresh_if_older)
        
        return None

    # TODO: ensure mutual exclusion for mirroring
    def mirror_all(self):
        with utils.Stopwatch(f'Fetch all records from {self.compositeName()}'):
            mirroredTime = datetime.datetime.now(tz=pytz.utc)
            recs = self.table.get_all(view=self.view)
        with utils.Stopwatch(f'Upsert all records to postgresql, mirror timestamp {mirroredTime}'):
            with self.engine.connect() as con:
                self.upsert(con, recs, mirroredTime=mirroredTime)

                # Update mirroredTime for this table to be the mirroredTime
                # we stored right before the call to get_all
                self._setMirroredTime(con, mirroredTime)

                # This isn't a good test for needing to delete records because
                # there can be multiple tables, but we will need to deal with 
                # cleanup.  Search for Record counts and ponder proper structure.
                # if con.scalar(mirror_table.count()) > len(recs):
                #     print(f'Need to delete some records')

        # Update key_map
        if self.key_col:
            self.update_key_map_from_psql()
                    
        return {rec['id']:rec for rec in recs}

    def mirroredTime(self, con):
        rec = con.execute(select([metadata_table])
            .where(metadata_table.c.baseKey == self.baseKey)
            .where(metadata_table.c.tablename == self.tablename)
            .where(metadata_table.c.view == self.view)).fetchone()
        return rec and rec['mirroredTime']
    
    def _setMirroredTime(self, con, mirroredTime):
        record = self.db_indices()
        record['mirroredTime'] = mirroredTime
        ins = psql_insert(metadata_table)
        upsert = ins.on_conflict_do_update(
            constraint=metadata_table.primary_key,
            set_=dict(mirroredTime=ins.excluded.mirroredTime))
        con.execute(upsert, [record])

    # TODO: ensure mutual exclusion for mirroring
    def incremental_mirror(self, force_all=False,keep_id_arr=None):
        with self.engine.connect() as con:
            get_all_opts = dict(view=self.view)
            msg = f'incremental_mirror fetching all records {self.compositeName()}'
            if not force_all:
                last_mirroredTime = self.mirroredTime(con)
                if last_mirroredTime:
                    last_mirroredTime -= datetime.timedelta(hours=1)
                    msg = f'incremental_mirror fetching records modified since {last_mirroredTime} {self.compositeName()}'
                    get_all_opts['formula'] = "{Last modified time} >= '%s'" % last_mirroredTime.isoformat()
            with utils.Stopwatch(msg):
                this_mirroredTime = datetime.datetime.now(pytz.utc)
                recs = self.table.get_all(**get_all_opts)
            with utils.Stopwatch(f'Upsert recent records'):
                self.upsert(con, recs, this_mirroredTime)
            self._setMirroredTime(con, this_mirroredTime)
    
        if self.key_col:
            self.update_key_map_from_psql(keep_id_arr=keep_id_arr)

    # If keep_id_arr is provided, then records with id's not in it
    # are not put into key_map.  Otherwise all records are.
    # This is to deal with preserving records that have been pruned from airtables
    
    def update_key_map_from_psql(self,keep_id_arr=None):
        if not self.key_col:
            return None
        
        # Clear key_map
        self.key_map = {}
        
        with self.engine.connect() as con:
            kc = self.key_col
            print(f" Updating key_map for field {kc}")
            cmd = self.select()
            all_recs = [dict(x) for x in list(con.execute(cmd))]
            for rec in all_recs:
                if 'id' in rec and 'fields' in rec and kc in rec['fields']:
                    key = rec['fields'][kc]
                    if not keep_id_arr or rec['id'] in keep_id_arr:
                        if key in self.key_map:
                            print(f"  {self.tablename}: Warning key {key} occurs multiple times ({self.key_map[key]} and {rec['id']})")
                            continue
                            
                        self.key_map[key]=rec['id']
#                         print(f"  Adding key {key} = {rec['id']}")
#                     elif not keep_id_arr:
#                         print(f"  No key in {rec}")
#                     else:
#                         print(f"  Skipping {rec['id']}")
        return self.key_map

    # find_one_or_none(con, refresh_if_older=datetime.timedelta(minutes=5))
    # refresh_if_older is a datetime.timedelta OR an absolute datetime
    def find_one_or_none(self, con, id=None, refresh_if_older=None,verbose=False):        
        now = datetime.datetime.now(tz=pytz.utc)

        # Check if refresh_if_older is a datetime.timedelta or not
        if refresh_if_older and not isinstance(refresh_if_older,datetime.timedelta):
            use_delta = now-refresh_if_older
            if verbose:
                print(f"find_one_or_none: absolute refresh_if_older {refresh_if_older}, use timedelta {use_delta}")
            refresh_if_older = use_delta

        sel = self.select()
        if id:
            sel = sel.where(mirror_table.c.id == id)
        rec = con.execute(sel.limit(1)).fetchone()
        if rec:
            if refresh_if_older==None:
                if verbose:
                    print(f'find_one_or_none: record found, refresh_if_older=None')
                return dict(rec)
            else:
                if not id:
                    id = rec['id']
                #mirroredTime = rec.mirroredTime
                mirroredTime = rec['mirroredTime']
                table_mirroredTime = self.mirroredTime(con)
                if table_mirroredTime:
                    mirroredTime = max(mirroredTime, table_mirroredTime)
                age = now - mirroredTime
                if age <= refresh_if_older:
                    if verbose:
                        print(f'find_one_or_none: age {age} <= {refresh_if_older}, returning record')
                    return dict(rec)
                elif verbose:
                    print(f'find_one_or_none, age {age} > {refresh_if_older}, need to refresh {id} from {self.tablename}')
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> need to fetch from airtable!')
        # Fall-through:  either rec wasn't found in psql, or it's too old
        # If id is None, just return None
        if not id:
            return None
        
        # Do a retry; we keep getting weird, sporadic DECRYPTION_FAILED_OR_BAD_RECORD_MAC errors
        for retry in range(0,2):
            try:
                fresh_rec = self.table.get(id)
                #print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  Got {fresh_rec}')
            except requests.HTTPError as e:
                # Assume 404, no such record
                # (Tried looking at response code but couldn't make it work)
                print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  Failed to get airtable record on retry {retry}: {e}')
                fresh_rec = None
                # Wait half a second 
                time.sleep(.5)

            if fresh_rec:
                # This will mutate fresh_rec to insert the table fields
                self.upsert(con, [fresh_rec], now)
                
                # Update key_map
                if self.key_col:
                    kc = self.key_col
                    if 'id' in fresh_rec and 'fields' in fresh_rec and kc in fresh_rec['fields']:
                        key = fresh_rec['fields'][kc]
                        self.key_map[key]=fresh_rec['id']
                        print(f"  Adding key {key} = {fresh_rec['id']}")
                
                return fresh_rec

        print(f'>>>>>>>>>>>>>>>>>>>>>>>>> Failed to get record from psql or Airtable')
        return None

    def select(self):
        return (select([mirror_table])
            .where(mirror_table.c.baseKey == self.baseKey)
            .where(mirror_table.c.tablename == self.tablename)
            .where(mirror_table.c.view == self.view))

    # If view arg is different than self.view, read from Airtable instead of 
    # psql cache
    def get_all(self,view=None):
        if not view or view == self.view:
            print(f"Reading view {view} from psql mirror")
            with self.engine.connect() as con:
                cmd = self.select()
                all_recs = [dict(x) for x in list(con.execute(cmd))]

            return all_recs
        else:
            print(f"Reading view {view} from Airtable directly")
            return self.table.get_all(view=view)

# This represents an Airtable Base which has been modified to have a special table called 
# Record counts to help with incremental mirroring support.  It should have one row, with 
# columns named TN links, TN count, and TN last modified for each tablename to be mirrored
# 
# self.views is a defaultdict with primary key of tablename, secondary key of viewname, and 
# value of CachedAirtableView.

# self.metadata is a map from tablename to metadata about the table.  For tables which are 
# included in Record counts, this includes:
#   'recordCountState':
#      'rcMirroredTime', 'links', 'count', and 'lastModified'
# this is filled in initially in add_view with the data in self.rc_rec at the time of creation

class CachedAirtableBase:
    def __init__(self, baseName, cat_engine=None):
        self.baseName = baseName
        self.views = defaultdict(lambda:{})
        self.metadata = defaultdict(lambda:{})
        self.objCreatedTime = datetime.datetime.now(datetime.timezone.utc)

        assert cat_engine, "ERROR: Must provide cat_engine arg to create a CachedAirtableBase"
        self.engine = cat_engine

        # Create a view for Record counts
        self.recordCountView = CachedAirtableView(baseName, 'Record counts', 'Grid view', cat_engine=self.engine)
        self.rc_rec = None
        
    # refresh_if_older is a datetime.timedelta
    def get_record_counts_rec(self, refresh_if_older=None):     
        now = datetime.datetime.now(tz=pytz.utc)
        # Default to forcing a refresh from Airtable on first call after object creation, or 
        # 1 minute, whichever is less, if refresh_if_older is not specified
        if not refresh_if_older:
            refresh_if_older = min(datetime.timedelta(minutes=1),now - self.objCreatedTime)
            
        with self.engine.connect() as con:
            # Check if a version of rc_rec is in postgres
            if not self.rc_rec:
                # There should just be one record, get it
                psql_rec = self.recordCountView.find_one_or_none(con, refresh_if_older=refresh_if_older)
                if psql_rec:
                    #print(f"############### CachedAirtableBase got rc_rec from recordCountView, id={psql_rec['id']}, mirroredTime={psql_rec['mirroredTime']}")
                    self.rc_rec = psql_rec
                    return psql_rec
                else:
                    # Could be initial run, get 'all' from Airtable
                    print("Record counts not in postgres yet, mirroring all from Airtable")
                    at_recs = self.recordCountView.mirror_all()
                    if len(at_recs)>=1:
                        self.rc_rec = at_recs[list(at_recs.keys())[0]]
                        return self.rc_rec
            else:
                # We already have an rc_rec, check if it's fresh enough
                #mirroredTime = self.rc_rec.mirroredTime
                mirroredTime = self.rc_rec['mirroredTime']
                age = now - mirroredTime
                if age <= refresh_if_older:
                    print(f'get_record_counts_rec: age {age} <= {refresh_if_older}, returning record')
                    return self.rc_rec
                else:
                    print(f"get_record_counts_rec, age {age} > {refresh_if_older}, need to refresh {self.rc_rec['id']} from recordCountView")
                    psql_rec = self.recordCountView.find_one_or_none(con, refresh_if_older=refresh_if_older)
                    if psql_rec:
                        #print(f"############### CachedAirtableBase got rc_rec from recordCountView, id={psql_rec['id']}, mirroredTime={psql_rec['mirroredTime']}")
                        self.rc_rec = psql_rec
                        return psql_rec

        assert False, "ERROR in get_record_counts_rec: Failed to fetch Record count record"

    # Store a CachedAirtableView for a given table
    def add_view(self, tablename, viewname, key_col=None):
        new_view = CachedAirtableView(self.baseName, tablename, viewname,key_col=key_col,cat_engine=self.engine)
        self.views[tablename][viewname] = new_view

        return new_view

    # Retrieve a stored view; defaults to Grid view
    def get_view(self, tablename, viewname=None):
        if not viewname:
            viewname = 'Grid view'
        if tablename in self.views and viewname in self.views[tablename]:
            return self.views[tablename][viewname]
        else:
            print(f"get_view failed to find {tablename}, {viewname} in self.views")
            return None

    def update_rc_metadata_for_view(self, tablename):
        # Check if this table is in Record counts
        last_mod_fname = f"{tablename} last modified"
        count_fname = f"{tablename} count"
        if count_fname in self.rc_rec['fields']:
            if self.rc_rec['fields'][count_fname] > 0 and last_mod_fname in self.rc_rec['fields']:
                #print(f"############### CachedAirtableBase added {tablename}: rc_rec last modified = {self.rc_rec['fields'][last_mod_fname]} as of {self.rc_rec['mirroredTime']}")
                view_metadata = {
                    'rcMirroredTime': self.rc_rec['mirroredTime'],
                    'lastModified': dateutil.parser.isoparse(self.rc_rec['fields'][last_mod_fname])
                }
                for topic in ['links', 'count']:
                    t_fname = f'{tablename} {topic}'
                    if t_fname in self.rc_rec['fields']:
                        view_metadata[topic] = self.rc_rec['fields'][t_fname]
            else:
                # Table is empty
                view_metadata = {
                    'rcMirroredTime': self.rc_rec['mirroredTime'],
                    'lastModified': self.rc_rec['mirroredTime']-datetime.timedelta(seconds=1),
                    'count':0,
                    'links':[]
                }
                        
            self.metadata[tablename] = view_metadata
            #print("  %r"%({k:v for k,v in view_metadata.items() if k!='links'}))
            return view_metadata

        assert False, f"update_rc_metadata_for_view: Asking for tablename {tablename} not in rc_rec {self.rc_rec}"

    # Only incrementally mirror tables that have actually changed according to the Record count record
    # Note: This knows all it would need to know for deleting old records, but it doesn't 
    # refresh_if_older is a datetime.timedelta
    def incremental_mirror_all(self, refresh_if_older=None):
        # Make sure we have a sufficiently fresh verson of the Record counts rec
        rc_rec = self.get_record_counts_rec()
        rc_mirroredTime = rc_rec['mirroredTime']

        with self.engine.connect() as con:
            # For each view, check metadata
            for tablename, view_dict in self.views.items():
                # This gets info from the Airtable Record count table about this tablename
                view_metadata = self.update_rc_metadata_for_view(tablename)

                for viewname, view_obj in view_dict.items(): 
                    # Get the info on when this Airtable table was last mirrored into postgres
                    postgres_mirroredTime = view_obj.mirroredTime(con)
                    
                    if not postgres_mirroredTime:
                        print(f"incremental_mirror_all INITIALIZING {tablename} for the first time")
                        ret = view_obj.mirror_all()
                        print(f"  DONE: {len(ret)} records in {tablename}")
                        continue

                    # We potentially have more items in postgres than are currently active in Airtable.
                    # Set view's key map
                    curr_view_id_arr = list(view_metadata['links'])
                    
                    if postgres_mirroredTime < view_metadata['lastModified']:
                        # Airtable table was modified more recently than it was last mirrored, update it
                        print(f"incremental_mirror_all UPDATING {tablename}: lastModified in Airtable = {view_metadata['lastModified']} > postgres mirroredTime = {postgres_mirroredTime} [rc mirroredTime={rc_mirroredTime}], {len(curr_view_id_arr)} active keys")
                        view_obj.incremental_mirror(keep_id_arr=curr_view_id_arr)
                    else:
                        # The contents of Airtable tablename hasn't been modified, and we confirmed that as of rc_mirroredTime.
                        # Update view_obj's mirroredTiem to be rc_mirroredTime
                        view_obj._setMirroredTime(con,rc_mirroredTime)
                        print(f"incremental_mirror_all keeping {tablename}: lastModified in Airtable = {view_metadata['lastModified']} <= postgres mirroredTime = {postgres_mirroredTime}, updated to rc mirroredTime={rc_mirroredTime}, {len(curr_view_id_arr)} active keys")
                        
                        # We potentially have more items in postgres than are currently active in Airtable.
                        # Set view's key map
                        curr_view_key_map = view_obj.update_key_map_from_psql(keep_id_arr=curr_view_id_arr)
                        if curr_view_key_map:
                            print(f"  {tablename} found {len(curr_view_key_map)} keys")
