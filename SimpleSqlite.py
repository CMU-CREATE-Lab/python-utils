import datetime, os, sqlite3, time

try:
    import thread
except:
    import threading as thread
    
class SimpleSqlite:
    def __init__(self, database_path):
        self._database_path = _database_path
        self._log_path = self._database_path + '.log'
        self._log_file = open(self._log_path, 'a')
        self._max_retries = 5 # Retry locking this many times
        self._lock_delay_secs = 2  # Sleep this long between attempts to lock
        self._conns = {}

    def execute_write(self, cmd, args=()):
        num_retries = 0
        self._log('execute_write start %s' % cmd)

        while num_retries < self._max_retries:
            try:
                before = time.time()
                self._get_conn().execute(cmd, args)
                self._log('execute_write completed, duration %.1f ms' % (1000 * (time.time() - before)))
                return True
            except sqlite3.OperationalError as e:
                self._log('execute_write generated sqlite3.OperationalError %s, duration %.1f ms, num_retries=%d, sleeping %d seconds' % (e, 1000 * (time.time() - before), num_retries, self._lock_delay_secs))
                num_retries += 1
                time.sleep(self._lock_delay_secs)
                self._log('execute_write start retry %s' % cmd)
                continue
            except Exception as e:
                self._log('execute_write generated exception %s, duration %.1f ms' % (e, 1000 * (time.time() - before)))
                raise
        self._log('execute_write %s failed after %d retries, duration %.1f ms' % (cmd, num_retries, 1000 * (time.time() - before)))
        return False

    def execute_read_fetchall(self, cmd, args=()):
        num_retries = 0
        self._log('execute_read_fetchall start %s' % cmd)

        while num_retries < self._max_retries:
            try:
                before = time.time()
                rows = self._get_conn().execute(cmd, args).fetchall()
                self._log('execute_read_fetchall completed, duration %.1f ms' % (1000 * (time.time() - before)))
                return rows
            except sqlite3.OperationalError as e:
                self._log('execute_read_fetchall sqlite3.OperationalError %s, duration %.1f ms, num_retries=%d, sleeping %d seconds' % (e, 1000 * (time.time() - before), num_retries, self._lock_delay_secs))
                num_retries += 1
                time.sleep(self._lock_delay_secs)
                self._log('execute_read_fetchall start retry %s' % cmd)
                continue
            except Exception as e:
                self._log('execute_read_fetchall generated exception %s, duration %.1f ms' % (e, 1000 * (time.time() - before)))
                raise
                
        self._log('execute_read_fetchall %s failed after %d retries, duration %.1f ms' % (cmd, num_retries, 1000 * (time.time() - before)))
        return None
    
    def _get_TPID(self):
        return '%d.%s' % (os.getpid(), thread.get_ident())

    def _log(self, msg):
        self._log_file.write('%s %s: %s\n' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], self.getTPID(), msg))
        self._log_file.flush()

    def _get_conn(self):
        if not self._get_TPID() in self._conns:
            self._log('Opening %s' % self._database_path)
            before = time.time()
            self._conns[self._get_TPID()] = sqlite3.connect(self._database_path)
            self._log('open completed, duration %.1f ms' % (1000 * (time.time() - before)))
        return self._conns[self._get_TPID()]
