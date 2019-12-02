import datetime, os, sqlite3, time

try:
    import thread
except:
    import threading as thread

class SimpleSqlite:
    def __init__(self, database_path):
        self._database_path = database_path
        self._log_path = self._database_path + '.log'
        self._log_file = open(self._log_path, 'a')
        self._max_retries = 5 # Retry locking this many times
        self._lock_delay_secs = 2  # Sleep this long between attempts to lock
        self._conns = {}
        
    def _perform_with_retries(self, operation_name, fn):
        self._log('%s start' % (operation_name))

        for num_retries in range(0, self._max_retries):
            try:
                before = time.time()
                ret = fn()
                self._log('%s completed, duration %.1f ms' % 
                          (operation_name, 1000 * (time.time() - before)))
                return ret
            except sqlite3.OperationalError as e:
                self._log('%s generated sqlite3.OperationalError %s, duration %.1f ms, num_retries=%d' % 
                          (operation_name, e, 1000 * (time.time() - before), num_retries))
                if num_retries < self._max_retries - 1:
                    self._log('Sleeping %f seconds before retrying' % self._lock_delay_secs)
                    time.sleep(self._lock_delay_secs)
                    continue
                else:
                    self._log('Too many retries, raising exception')
                    raise
            except Exception as e:
                self._log('%s generated exception %s, duration %.1f ms' % 
                          (operation_name, e, 1000 * (time.time() - before)))
                raise

    def execute_write(self, cmd, args=()):
        def write_fn():
            self._get_conn().execute(cmd, args)
            self._get_conn().commit()
            return True
        
        return self._perform_with_retries('execute_write(%s)' % cmd, write_fn)

    def execute_read_fetchall(self, cmd, args=()):
        def read_fetchall_fn():
            return self._get_conn().execute(cmd, args).fetchall()
            
        return self._perform_with_retries('execute_read_fetchall(%s)' % cmd, read_fetchall_fn)
    
    def execute_read_fetchall_dicts(self, cmd, args=()):
        def read_fetchall_dicts_fn():
            cursor = self._get_conn().execute(cmd, args)
            keys = [col[0] for col in cursor.description]
            return [dict(zip(keys, row)) for row in cursor.fetchall()]
        
        return self._perform_with_retries('execute_read_fetchall_dicts(%s)' % cmd, read_fetchall_dicts_fn)

    def get_table_column_names(self, table_name):
        return set(self.execute_read_fetchall_dicts('SELECT * FROM %s LIMIT 1;' % table_name)[0].keys())
    
    def add_column_if_not_exists(self, table_name, column_name, column_type):
        if not column_name in self.get_table_column_names(table_name):
            db.execute_write('ALTER TABLE %s ADD COLUMN %s %s;' % (table_name, column_name, column_type))

    def _get_TPID(self):
        return '%d.%s' % (os.getpid(), thread.get_ident())

    def _log(self, msg):
        self._log_file.write('%s %s: %s\n' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], self._get_TPID(), msg))
        self._log_file.flush()

    def _get_conn(self):
        if not self._get_TPID() in self._conns:
            self._log('Opening %s' % self._database_path)
            before = time.time()
            self._conns[self._get_TPID()] = sqlite3.connect(self._database_path)
            self._log('open completed, duration %.1f ms' % (1000 * (time.time() - before)))
        return self._conns[self._get_TPID()]
