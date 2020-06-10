#%%

import concurrent, concurrent.futures, datetime, importlib, math, os, requests
import shutil, subprocess, sys, time, threading, traceback
try:
    import dateutil, dateutil.tz
except:
    pass

def reload_module(module_name):
    if module_name in sys.modules:
        importlib.reload(sys.modules[module_name])

def subprocess_check(*args, **kwargs):
    verbose = kwargs.pop('verbose', False)
    ignore_error = kwargs.pop('ignore_error', False)
    if len(args) == 1 and type(args[0]) == str:
        kwargs['shell'] = True
        if verbose:
            print(args[0])
    elif verbose:
        print(' '.join(args[0]))
    p = subprocess.Popen(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    (out, err) = p.communicate()
    out = out.decode('utf8')
    err = err.decode('utf8')
    ret = p.wait()
    if ret != 0 and not ignore_error:
        raise Exception(
            ('Call to subprocess_check failed with return code {ret}\n'
             'Standard error:\n{err}'
             'Standard out:\n{out}').format(**locals()))
    if len(err) > 0 and len(out) > 0 and err[-1] != '\n':
        err += '\n'
    all = err + out
    if verbose and all.strip():
        print(all.strip())
    return all

def download_file(url, filename, timeout=3600, make_parents=True):
    if os.path.exists(filename):
        sys.stdout.write('%s already downloaded\n' % filename)
        return True
    else:
        dirname = os.path.dirname(filename)
        print('dirname is', dirname)
        if dirname and make_parents and not os.path.exists(dirname):
            os.makedirs(dirname)
        sys.stdout.write('Downloading %s to %s\n' % (url, filename))

        try:
            response = requests.Session().get(url, timeout=timeout)
            if(response.status_code!=200):
                print('Error response, code = %d, body = %s' % (response.status_code, response.text))
                return False
        except requests.exceptions.RequestException as e:
            sys.stdout.write("Couldn't read %s because %s" % (url, e))
            return False

        open(filename + '.tmp', "wb").write(response.content)
        os.rename(filename + '.tmp', filename)
        sys.stdout.write('Done, wrote %d bytes to %s\n' % (len(response.content), filename))
        return True

def unzip_file(filename):
    exdir = os.path.splitext(filename)[0]
    if os.path.exists(exdir):
        sys.stdout.write('%s already unzipped\n' % (filename))
    else:
        tmpdir = exdir + '.tmp'
        shutil.rmtree(tmpdir, True)
        sys.stdout.write('Unzipping %s into %s\n' % (filename, tmpdir))
        subprocess_check(['unzip', filename, '-d', tmpdir])
        os.rename(tmpdir, exdir)
        print('Success, created %s' % exdir)
    return exdir

def gunzip_file(filename):
    dest = os.path.splitext(filename)[0]
    if os.path.exists(dest):
        sys.stdout.write('%s already unzipped\n' % (filename))
    else:
        tmp = dest + '.tmp'
        sys.stdout.write('gunzipping %s\n' % (filename))
        subprocess.check_call("gunzip -c '%s' > '%s'" % (filename, tmp), shell=True)
        os.rename(tmp, dest)
        sys.stdout.write('Success, created %s\n' % (dest))
    
class SimpleThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers):
        super(SimpleThreadPoolExecutor, self).__init__(max_workers=max_workers)
        self.futures = []

    def submit(self, fn, *args, **kwargs):
        future = super(SimpleThreadPoolExecutor, self).submit(fn, *args, **kwargs)
        self.futures.append(future)
        return future

    def get_futures(self):
        return self.futures

    def shutdown(self):
        exception_count = 0
        results = []
        for completed in concurrent.futures.as_completed(self.futures):
            try:
                results.append(completed.result())
            except Exception as e:
                exception_count += 1
                sys.stderr.write(
                    'Exception caught in SimpleThreadPoolExecutor.shutdown.  Continuing until all are finished.\n' +
                    'Exception follows:\n' +
                    traceback.format_exc())
        super(SimpleThreadPoolExecutor, self).shutdown()
        if exception_count:
            raise Exception('SimpleThreadPoolExecutor failed: %d of %d raised exception' % (exception_count, len(self.futures)))
        print('SimpleThreadPoolExecutor succeeded: all %d jobs completed' % len(self.futures))
        return results


class SimpleProcessPoolExecutor(concurrent.futures.ProcessPoolExecutor):
    def __init__(self, max_workers):
        super(SimpleProcessPoolExecutor, self).__init__(max_workers=max_workers)
        self.futures = []

    def submit(self, fn, *args, **kwargs):
        future = super(SimpleProcessPoolExecutor, self).submit(fn, *args, **kwargs)
        self.futures.append(future)
        return future

    def get_futures(self):
        return self.futures

    def shutdown(self):
        exception_count = 0
        results = []
        for completed in concurrent.futures.as_completed(self.futures):
            try:
                results.append(completed.result())
            except Exception as e:
                exception_count += 1
                sys.stderr.write(
                    'Exception caught in SimpleProcessPoolExecutor.shutdown.  Continuing until all are finished.\n' +
                    'Exception follows:\n' +
                    traceback.format_exc())
        super(SimpleProcessPoolExecutor, self).shutdown()
        if exception_count:
            raise Exception('SimpleProcessPoolExecutor failed: %d of %d raised exception' % (exception_count, len(self.futures)))
        print('SimpleProcessPoolExecutor succeeded: all %d jobs completed' % len(self.futures))
        return results

    def kill(self, signal=9):
        for pid in self._processes.keys():
            print('Killing %d with signal %d' % (pid, signal))
            os.kill(pid, signal)

class Stopwatch:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.start = time.time()
    def __exit__(self, type, value, traceback):
        sys.stdout.write('%s took %.1f seconds\n' % (self.name, time.time() - self.start))
        sys.stdout.flush()

def sleep_until_next_period(period, offset=0):
    now = time.time()
    start_of_next_period = math.ceil((now - offset) / period) * period + offset
    delay = start_of_next_period - now
    print('sleep_until_next_period(%d, %d) sleeping %d seconds until %s' % 
          (period, offset, delay, datetime.datetime.fromtimestamp(start_of_next_period).strftime('%H:%M:%S')))
    time.sleep(delay)

def formatSecs(secs):
    if secs < 60:
        return '%d secs' % secs

    mins = secs / 60
    if mins < 60:
        return '%.1f mins' % mins

    hours = mins / 60;
    if hours < 24:
        return '%.1f hrs' % hours

    days = hours / 24
    return '%.1f days' % days

class StatInstance:
    def __init__(self, use_staging_server=False):
        if use_staging_server:
            self.server_hostname = 'stat-staging.createlab.org'
        else:
            self.server_hostname = 'stat.createlab.org'
        self.hostname = None
        self.service = None

    def get_datetime(self):
        return datetime.datetime.now(dateutil.tz.tzlocal()).isoformat()

    def get_hostname(self):
        if not self.hostname:
            self.hostname = subprocess_check('hostname').strip()
        return self.hostname

    def set_service(self, service):
        self.service = service

    # Possible levels include 'up', 'down', 'info', 'debug', 'warning', critical'
    def log(self, service, level, summary, details=None, host=None, payload={}, valid_for_secs=None, shortname=None):
        service = service or self.service
        if not service:
            raise Exception('log: service must be passed, or set previously with set_service')            
        host = host or self.get_hostname()
        post_body = {
                'service': service,
                'datetime': self.get_datetime(),
                'host': host,
                'level': level,
                'summary': summary,
                'details': details,
                'payload': payload,
                'valid_for_secs': valid_for_secs,
                'shortname': shortname
            }
        print('Stat.log %s %s %s %s %s' % (level, service, host, summary, details))
        sys.stdout.flush()
        timeoutInSecs = 20
        try:
            response = requests.post('https://%s/api/log' % self.server_hostname,
                                     json=post_body, timeout=timeoutInSecs)
            if response.status_code != 200:
                sys.stderr.write('POST to https://stat.createlab.org/api/log failed with status code %d and response %s' % (response.status_code, response.text))
                sys.stderr.flush()
                return
        except requests.exceptions.RequestException:
            sys.stderr.write('POST to https://stat.createlab.org/api/log timed out')
            sys.stderr.flush()

    def info(self, summary, details=None, payload={}, host=None, service=None, shortname=None):
        self.log(service, 'info', summary, details=details, payload=payload, host=host, shortname=shortname)

    def debug(self, summary, details=None, payload={}, host=None, service=None, shortname=None):
        self.log(service, 'debug', summary, details=details, payload=payload, host=host, shortname=shortname)

    def warning(self, summary, details=None, payload={}, host=None, service=None, shortname=None):
        self.log(service, 'warning', summary, details=details, payload=payload, host=host, shortname=shortname)

    def critical(self, summary, details=None, payload={}, host=None, service=None, shortname=None):
        self.log(service, 'critical', summary, details=details, payload=payload, host=host, shortname=shortname)

    def up(self, summary, details=None, payload={}, valid_for_secs=None, host=None, service=None, shortname=None):
        self.log(service, 'up', summary,
                 details=details, payload=payload, valid_for_secs=valid_for_secs, host=host, shortname=shortname)

    def down(self, summary, details=None, payload={}, valid_for_secs=None, host=None, service=None, shortname=None):
        self.log(service, 'down', summary,
                 details=details, payload=payload, valid_for_secs=valid_for_secs, host=host, shortname=shortname)

Stat = StatInstance()

# ThCall(func, *args, **kwargs) calls func(*args, **kwargs) in a separate thread
# value() waits for func to complete and returns its value
class ThCall(threading.Thread):
    def __init__(self, func, *args, **kwargs):
        self._exc_info = None
        def runner():
            try:
                self._value = func(*args, **kwargs)
            except:
                self._exc_info = sys.exc_info()
                print('ThCall caught exception:')
                traceback.print_exception(*self._exc_info)
        super().__init__(target=runner)
        self.start()
    
    def value(self):
        if self.is_alive():
            self.join()
        if self._exc_info:
            raise self._exc_info[0]
        else:
            return self._value



# %%
