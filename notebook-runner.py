import argparse, datetime, fcntl, json, os, re, requests, subprocess, sys, threading


parser = argparse.ArgumentParser()
parser.add_argument('watchdirs', nargs='+', help='Directories in which to recursively find *.autorun.ipynb')
args = parser.parse_args()


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

script_dir = os.path.dirname(__file__)

exec_ipynb(script_dir + '/utils.ipynb')
python_path = sys.executable
run_notebook_path = os.path.realpath(script_dir + '/run-notebook-2.py')
dirs_to_watch = [os.path.realpath(dir) for dir in args.watchdirs]


def log(msg):
    sys.stdout.write('NR %s\n' % msg)
    sys.stdout.flush()

log('Using %s %s' % (python_path, run_notebook_path))
log('Watching directories %s' % dirs_to_watch)
notebook_timeout = 0

class NotebookManager:
    def __init__(self, notebook):
        self.notebook = notebook
        self.pid = None
        log('Managing %s' % notebook)
        self.shutting_down = False
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    # Run from self.thread
    # Keeps running notebook repeatedly
    def run(self):
        while not self.shutting_down:
            cmdline = [python_path,
                       run_notebook_path,
                       self.notebook,
                       '--timeout', str(notebook_timeout)]
            log('%s: starting %s' % (self.notebook, ' '.join(cmdline)))

            self.proc = subprocess.Popen(cmdline)
            ret = self.proc.wait()
            log('%s: process ended with status %d' % (self.notebook, ret))
            if self.shutting_down:
                break
            log('%s: retrying at the start of next minute' % self.notebook)
            sleep_until_next_period(60)
        log('%s: shutting down run loop' % self.notebook)

managed_notebooks = {}

def get_notebook_manager(notebook):
    if not notebook in managed_notebooks:
        managed_notebooks[notebook] = NotebookManager(notebook)
    return managed_notebooks[notebook]

inotify = subprocess.Popen([
        'inotifywait',
        '--recursive',
        '--monitor',
        '--event', 'CLOSE_WRITE,MOVED_TO',
        '--format', '%e %w%f'] + dirs_to_watch,
        stdout=subprocess.PIPE)

initial_notebooks = subprocess_check(['find'] + dirs_to_watch + ['-name', '*.autorun.ipynb'] ).strip().split('\n')
initial_notebooks = [n in initial_notebooks if n] # remove blank lines, ugh
log('Starting %d initial notebooks %s' % (len(initial_notebooks), initial_notebooks))

for notebook in initial_notebooks:
    get_notebook_manager(os.path.realpath(notebook))

log('Watching for new notebooks')
while True:
    (events, path) = inotify.stdout.readline().decode('utf-8').strip().split(' ', 1)
    print(events, path)

