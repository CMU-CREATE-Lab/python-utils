#!/bin/env python

import argparse, codecs, datetime, fcntl, io, json, os, pwd, re, requests, signal, subprocess, sys, time, traceback

parser = argparse.ArgumentParser()
parser.add_argument('notebook', help='Notebook to run')
parser.add_argument('--email-on-fail', default='randy.sargent@gmail.com', help='Email this address on fail or timeout')
parser.add_argument('--timeout', type=int, default=1800, help='Timeout, in seconds.  (Kill notebook if it runs this long.)')
args = parser.parse_args()

notebook_path = os.path.abspath(sys.argv[1])


class LoggingStream(io.TextIOBase):
    def __init__(self, out):
        self.remainder = ''
        self.out = out

    def write(self, str):
        lines = (self.remainder + str).split('\n')
        self.remainder = lines[-1]
        for line in lines[0:-1]:
            fmt_date = datetime.datetime.now().isoformat(' ', 'milliseconds')
            self.out.write('%s %d: %s\n' % (fmt_date,
                                            os.getpid(),
                                            line))
        self.out.flush()
            
    def __del__(self):
        if self.remainder:
            self.write('\n')
    
log_path = notebook_path + '.log'

logfile = open(log_path, 'a')

sys.stdout = sys.stderr = LoggingStream(logfile)

print('notebook-service.py %s' % args.notebook)

def notebook_succeeded(msg):
    print(msg)
    #if os.path.exists('/usr/local/datadog/config.json'):
    #    datadog_operation_succeeded(message=msg, tags=["notebook:" + os.path.basename(notebook_path)])
    #    logfile.write(header() + ': sent to datadog\n')

def notebook_failed(msg):
    print(msg)
    #if os.path.exists('/usr/local/datadog/config.json'):
    #    datadog_operation_failed(message=msg, tags=["notebook:" + os.path.basename(notebook_path)])
    #    logfile.write(header() + ': sent to datadog\n')
    #else:
    #    sendMail([args.email_on_fail], 'FAILED: %s' % notebook_path, message)
    #    logfile.write(header() + ': emailed\n')

def run_notebook():
    run_notebook_start_time = time.time()

    def get_username():
        try:
            return pwd.getpwuid(os.getuid())[0]
        except:
            return None

    print('Running as user %s\n' % get_username())
    print('Using python %s\n' % sys.executable)
    print('run-notebook.py is %s\n' % __file__)
    print('PATH: %s\n' % os.environ['PATH'])
    print('Home directory: %s\n' % os.path.expanduser('~'))
    
    lock_path = notebook_path + '.lock'

    try:
        lockfile = open(lock_path, 'w')
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print('Instance of %s is already running (%s locked).  Exiting.\n' % (notebook_path, lock_path))
            return 0

    
        def exec_ipynb(filename_or_url):
            nb = (requests.get(filename_or_url).json() if re.match(r'https?:', filename_or_url) else json.load(open(filename_or_url)))
            if(nb['nbformat'] >= 4):
                src = [''.join(cell['source']) for cell in nb['cells'] if cell['cell_type'] == 'code']
            else:
                src = [''.join(cell['input']) for cell in nb['worksheets'][0]['cells'] if cell['cell_type'] == 'code']
            exec('\n'.join(src), globals())
    
        try:
            os.chdir(os.path.dirname(notebook_path))
            exec_ipynb(notebook_path)
        except:
            message = 'Caught exception trying to run notebook %s: %s\n' % (notebook_path, sys.exc_info()[0])
            message += traceback.format_exc()

            notebook_failed('run-notebook.py %s failed after %d seconds with message %s\n' % (args.notebook, time.time() - run_notebook_start_time, message))
            return

        notebook_succeeded('run-notebook.py %s completed successfully after %d seconds' % (args.notebook, time.time() - run_notebook_start_time))
    finally:
        fcntl.flock(lockfile, fcntl.LOCK_UN)

run_notebook()
