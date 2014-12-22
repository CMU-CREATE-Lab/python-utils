#!/usr/bin/python

import codecs, datetime, fcntl, json, os, pwd, re, sys, time, traceback, urllib2


def run_notebook():
    run_notebook_start_time = time.time()

    def header():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + (', pid(%d)' % os.getpid())

    notebook_path = os.path.abspath(sys.argv[1])

    log_path = notebook_path + '.log'
    logfile = codecs.open(log_path, 'a', encoding='utf-8')
    logfile.write(header() + ': run-notebook.py %s\n' % sys.argv[1])
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
    logfile.write('HOME: %s\n' % os.environ['HOME'])
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
            exec '\n'.join([''.join(cell['input']) for cell in json.loads(nb)['worksheets'][0]['cells'] if cell['cell_type'] == 'code']) in globals()
    
        try:
            os.chdir(os.path.dirname(notebook_path))
            exec_ipynb(notebook_path)
        except:
            logfile.write(header() + ': Caught exception trying to run notebook %s\n' % sys.exc_info()[0])
            logfile.flush()
            traceback.print_exc(file=logfile)
            logfile.flush()
            return 1
    
        logfile.write(header() + ': run-notebook.py %s completed successfully after %d seconds\n' % (sys.argv[1], time.time() - run_notebook_start_time))
        return 0
    finally:
        fcntl.flock(lockfile, fcntl.LOCK_UN)

def run_notebook_with_lock():
    try:
        lockfile = open('parse-achd.lockfile','w')
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print 'Instance of parse-and-upload-achd is already running.  Exiting.'
            return
        last_uploaded_pdf = find_last_uploaded_pdf()
        for pdf in sorted(glob.glob('mirror/*.PDF')):
            if force_reprocess or not last_uploaded_pdf or pdf > last_uploaded_pdf:
                process_pdf(pdf)
    finally:
        fcntl.flock(lockfile, fcntl.LOCK_UN)


sys.exit(run_notebook())

