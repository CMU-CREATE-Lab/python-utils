#!/usr/bin/python

import argparse, codecs, datetime, fcntl, json, os, pwd, re, signal, subprocess, sys, time, traceback, urllib2

def timeout(signum, frame):
    raise Exception('Notebook running too long (%d seconds, --timeout); killing' % args.timeout)

def sendMail(toAddrs, subject, body):
    p = subprocess.Popen(['/usr/sbin/sendmail', '-t', '-i'], stdin=subprocess.PIPE)
    message = 'To: %s\nSubject: %s\n\n%s' % (', '.join(toAddrs), subject, body)
    p.communicate(message)
    if p.wait():
        raise Exception('sendmail failed')

parser = argparse.ArgumentParser()
parser.add_argument('notebook', help='Notebook to run')
parser.add_argument('--email-on-fail', default='randy.sargent@gmail.com', help='Email this address on fail or timeout')
parser.add_argument('--timeout', type=int, default=1800, help='Timeout, in seconds.  (Kill notebook if it runs this long.)')
args = parser.parse_args()

signal.signal(signal.SIGALRM, timeout)
signal.alarm(args.timeout)

def run_notebook():
    run_notebook_start_time = time.time()

    def header():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + (', pid(%d)' % os.getpid())

    notebook_path = os.path.abspath(sys.argv[1])

    log_path = notebook_path + '.log'
    logfile = codecs.open(log_path, 'a', encoding='utf-8')
    logfile.write(header() + ': run-notebook.py %s\n' % args.notebook)
    logfile.flush()

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
            nb = (urllib2.urlopen(filename_or_url) if re.match(r'https?:', filename_or_url) else open(filename_or_url)).read()
            jsonNb = json.loads(nb)
            #check for the modified formatting of Jupyter Notebook v4
            if(jsonNb['nbformat'] == 4):
                exec '\n'.join([''.join(cell['source']) for cell in jsonNb['cells'] if cell['cell_type'] == 'code']) in globals()
            else:
                exec '\n'.join([''.join(cell['input']) for cell in jsonNb['worksheets'][0]['cells'] if cell['cell_type'] == 'code']) in globals()
    
        try:
            os.chdir(os.path.dirname(notebook_path))
            exec_ipynb(notebook_path)
        except:
            message = header() + ': Caught exception trying to run notebook %s: %s\n' % (notebook_path, sys.exc_info()[0])
            message += traceback.format_exc()
            logfile.write(message)
            logfile.flush()

            # Email error
            sendMail([args.email_on_fail], 'FAILED: %s' % notebook_path, message)
            return 1
    
        logfile.write(header() + ': run-notebook.py %s completed successfully after %d seconds\n' % (args.notebook, time.time() - run_notebook_start_time))
        return 0
    finally:
        fcntl.flock(lockfile, fcntl.LOCK_UN)

sys.exit(run_notebook())

