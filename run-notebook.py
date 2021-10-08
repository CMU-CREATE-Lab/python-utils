#!/usr/bin/env python

import argparse, codecs, datetime, fcntl, json, os, pwd, re, requests, signal, subprocess, sys, time, traceback

parser = argparse.ArgumentParser()
parser.add_argument('notebook', help='Notebook to run')
parser.add_argument('--email-on-fail', default='randy.sargent@gmail.com', help='Email this address on fail or timeout')
parser.add_argument('--timeout', type=int, default=1800, help='Timeout, in seconds.  (Kill notebook if it runs this long.)')
args = parser.parse_args()

notebook_path = os.path.abspath(sys.argv[1])
# Add notebook directory to python library path
sys.path.insert(0, os.path.dirname(notebook_path))

def header():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + (', pid(%d)' % os.getpid())

log_path = notebook_path + '.log'
logfile = codecs.open(log_path, 'a', encoding='utf-8', buffering=1)
logfile.write(header() + ': run-notebook.py %s\n' % args.notebook)
logfile.flush()

def timeout(signum, frame):
    raise Exception('Notebook running too long (%d seconds, --timeout); killing' % args.timeout)

def sendMail(toAddrs, subject, body):
    p = subprocess.Popen(['/usr/sbin/sendmail', '-t', '-i'], stdin=subprocess.PIPE)
    message = 'To: %s\nSubject: %s\n\n%s' % (', '.join(toAddrs), subject, body)
    p.communicate(message)
    if p.wait():
        raise Exception('sendmail failed')

datadog_ok = 0
datadog_warning = 1
datadog_critical = 2

def datadog_request(url, **params):
    datadog_params = json.load(open('/usr/local/datadog/config.json'))
    params['host_name'] = subprocess.check_output(['hostname']).decode('utf-8').strip()
    params['timestamp'] = time.time()
    response = requests.post(url, params=datadog_params, json=params)

def datadog_operation_succeeded(**params):
    datadog_request('https://api.datadoghq.com/api/v1/check_run', 
                    check="op.end",
                    status=datadog_ok,
                    **params)
    
def datadog_operation_failed(**params):
    datadog_request('https://api.datadoghq.com/api/v1/check_run', 
                    check="op.end",
                    status=datadog_critical,
                    **params)

def notebook_succeeded(msg):
    logfile.write(header() + ': ' + msg + '\n')
    if os.path.exists('/usr/local/datadog/config.json'):
        datadog_operation_succeeded(message=msg, tags=["notebook:" + os.path.basename(notebook_path)])
        logfile.write(header() + ': sent to datadog\n')

def notebook_failed(msg):
    logfile.write(header() + ': ' + msg + '\n')
    if os.path.exists('/usr/local/datadog/config.json'):
        datadog_operation_failed(message=msg, tags=["notebook:" + os.path.basename(notebook_path)])
        logfile.write(header() + ': sent to datadog\n')
    else:
        sendMail([args.email_on_fail], 'FAILED: %s' % notebook_path, message)
        logfile.write(header() + ': emailed\n')

signal.signal(signal.SIGALRM, timeout)
signal.alarm(args.timeout)

def run_notebook():
    run_notebook_start_time = time.time()


    def get_username():
        try:
            return pwd.getpwuid(os.getuid())[0]
        except:
            return None

    logfile.write('Running as user %s\n' % get_username())
    logfile.write('Using python %s\n' % sys.executable)
    logfile.write('run-notebook.py is %s\n' % __file__)
    logfile.write('PATH: %s\n' % os.environ['PATH'])
    logfile.write('Home directory: %s\n' % os.path.expanduser('~'))
    logfile.flush()
    
    lock_path = notebook_path + '.lock'

    try:
        lockfile = open(lock_path, 'w')
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            logfile.write('Instance of %s is already running (%s locked).  Exiting.\n' % (notebook_path, lock_path))
            logfile.flush()
            return 0

        
        sys.stdout = sys.stderr = logfile

        def exec_ipynb(filename_or_url):
            nb = (requests.get(filename_or_url).json() if re.match(r'https?:', filename_or_url) else json.load(open(filename_or_url)))
            if(nb['nbformat'] >= 4):
                src = [''.join(cell['source']) for cell in nb['cells'] if cell['cell_type'] == 'code']
            else:
                src = [''.join(cell['input']) for cell in nb['worksheets'][0]['cells'] if cell['cell_type'] == 'code']

            tmpname = '/tmp/%s-%s-%d.py' % (os.path.basename(filename_or_url),
                                    datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'),
                                    os.getpid())
            src = '\n\n\n'.join(src)
            open(tmpname, 'w').write(src)
            code = compile(src, tmpname, 'exec')
            exec(code, globals())
    
        try:
            os.chdir(os.path.dirname(notebook_path))
            exec_ipynb(notebook_path)
        except Exception as e:
            message = header() + ': Caught exception trying to run notebook %s: %s\n' % (notebook_path, sys.exc_info()[0])
            message += traceback.format_exc()

            # Email error
            notebook_failed('%s:%s\nExecution time %d seconds.\n%s\n' % (os.path.basename(args.notebook), e, time.time() - run_notebook_start_time, message))
            return

        notebook_succeeded('run-notebook.py %s completed successfully after %d seconds' % (args.notebook, time.time() - run_notebook_start_time))
    finally:
        fcntl.flock(lockfile, fcntl.LOCK_UN)

run_notebook()
