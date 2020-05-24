#%%
from psql import Psql
import psycopg2, unittest
import pandas as pd

class TestPsql(unittest.TestCase):
    def setUp(self):
        self.db = Psql(dbname='earthtime')

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

    # TODO: implement date parsing
    #def test_df_date(self):
    #    df = pd.DataFrame([dict(d=datetime.date(2020,1,1))])
    #    self.db.execute('DROP TABLE IF EXISTS test_df')
    #    self.db.create_empty_table_from_df('test_df', df)
    #    self.db.append_df_to_table(df, 'test_df')
    #    df2 = self.db.select_as_df('SELECT * FROM test_df')
    #    self.assertEquals(df.d[0], df2.d[0])
    #    self.assertTrue(df.equals(df2))

    def test_insert(self):
        self.db.execute('DROP TABLE IF EXISTS foo')
        self.db.execute('CREATE TABLE foo (i int8, txt text)')
        rec = dict(i=11, txt='hi')
        self.db.insert_record('foo', rec)
        rec2 = self.db.select_record('SELECT * FROM foo')
        self.assertEquals(rec, rec2)

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
