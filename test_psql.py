#%%
import json, os, psycopg2, threading, unittest
import pandas as pd

if 'reload_module' in vars():
    reload_module('utils')
from utils import *

reload_module('psql')
from psql import Psql

class TestPsql(unittest.TestCase):
    def setUp(self):
        db_config = os.path.join(os.path.dirname(__file__), '.test_psql_credentials.json')
        if os.path.exists(db_config):
            args = json.load(open(db_config))
        else:
            print(f'{db_config} does not exist; using default database settings')
            args={'dbname':'postgres'}
        self.db = Psql(**args)

    def test_sanitize_column_name(self):
        self.assertEqual(self.db.sanitize_column_name(' Four score, and 40 years ago...'), 'four_score_and_40_years_ago')
    
    def test_exceptions(self):
        self.db.execute('DROP TABLE IF EXISTS foobar')
        with self.assertRaises(psycopg2.errors.UndefinedTable):
            self.db.execute('DROP TABLE foobar')
        self.db.execute('CREATE TABLE foobar (a text, b text)')
        self.db.execute('DROP TABLE foobar')

    def test_transactions(self):
        with self.assertRaises(psycopg2.errors.UndefinedTable):
            with self.db.transaction():
                self.db.execute('DROP TABLE IF EXISTS foobar')
                self.db.execute('DROP TABLE foobar')
        with self.db.transaction():
            self.db.execute('CREATE TABLE foobar (a text, b text)')
            self.db.execute('DROP TABLE foobar')
        self.assertTrue(self.db._transaction_count == 0)
    
    def test_df(self):
        df = pd.DataFrame([
            dict(i=1, f=1.2, txt='hi', b=True),
            dict(i=3, f=2.4, txt='by', b=False)
        ])
        self.db.execute('DROP TABLE IF EXISTS test_df')
        self.db.create_empty_table_from_df('test_df', df)
        self.db.append_df_to_table(df, 'test_df')
        df2 = self.db.select_as_df('SELECT * FROM test_df')
        self.assertTrue(df.equals(df2))

        self.assertEquals('hi', self.db.select_record('SELECT * FROM test_df WHERE i=1')['txt'])

        with self.assertRaises(Exception):
            # more than one record
            self.db.select_record('SELECT * FROM test_df')

        with self.assertRaises(Exception):
            # no records
            self.db.select_record('SELECT * FROM test_df WHERE i=999')

        self.assertEquals('hi', self.db.select_record_or_none('SELECT * FROM test_df WHERE i=1')['txt'])

        with self.assertRaises(Exception):
            # more than one record
            self.db.select_record_or_none('SELECT * FROM test_df')

        self.assertEquals(None, self.db.select_record_or_none('SELECT * FROM test_df WHERE i=999'))

    def test_insert(self):
        self.db.execute('DROP TABLE IF EXISTS foo')
        self.db.execute('CREATE TABLE foo (i int8, txt text)')
        rec = dict(i=11, txt='hi')
        self.db.insert_record('foo', rec)
        rec2 = self.db.select_record('SELECT * FROM foo')
        self.assertEquals(rec, rec2)

    def test_threads(self):
        x = 0
        lock = threading.Lock()

        def read_x():
            return self.db.select_record("SELECT v from foo where k='x'")['v']

        def test(i):
            nonlocal x
            time.sleep(i * 0.3 % 1)
            with lock:
                self.assertEquals(x, read_x())
            time.sleep(i * 0.35 % 0.5)
            with lock:
                x += 1
                self.db.execute("UPDATE foo SET v = %s WHERE k = 'x'", (x,))
            with lock:
                self.assertEquals(x, read_x())
        
        def test_thread(i):
            test(i)
            with self.db.transaction():
                test(i)
            test(i)
        
        self.db.execute('DROP TABLE IF EXISTS foo')
        self.db.execute('CREATE TABLE foo (k text, v int)')
        self.db.execute("INSERT INTO foo VALUES ('x', 0)")
        thcalls = [ThCall(test_thread, i) for i in range(10)]
        [thcall.value() for thcall in thcalls]
        self.assertEquals(30, read_x())


        # ThCall(func, *args, **kwargs) calls func(*args, **kwargs) in a separate thread
# value() waits for func to complete and returns its value

if __name__ == '__main__':
    if 'get_ipython' in vars():
        test_suite = unittest.TestSuite()
        test_suite.addTest(unittest.makeSuite(TestPsql))
        unittest.TextTestRunner().run(test_suite)
    else:
        unittest.main()

# %%
__name__ == '__main__'

# %%
