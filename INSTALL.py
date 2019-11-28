#!/usr/bin/env python3

import concurrent.futures, datetime, glob, json, os, re, shlex, subprocess

def exec_ipynb(filename_or_url):
    nb = (requests.get(filename_or_url).json() if re.match(r'https?:', filename_or_url) else json.load(open(filename_or_url)))
    if(nb['nbformat'] >= 4):
        src = [''.join(cell['source']) for cell in nb['cells'] if cell['cell_type'] == 'code']
    else:
        src = [''.join(cell['input']) for cell in nb['worksheets'][0]['cells'] if cell['cell_type'] == 'code']
    exec('\n'.join(src), globals())

exec_ipynb(os.path.dirname(os.path.realpath(__file__)) + '/utils.ipynb')

if subprocess_check('whoami').strip() != 'root':
    print('Please run INSTALL.py as root')
    exit(1)

# Change dir to dir of script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def without_backup_files(files):
    return [file for file in files if not file.endswith('~') and not file.startswith('#')]

srcs = without_backup_files(glob.glob('*'))
errs = 0

moveaside_suffix = '-moveaside=' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

install_scripts = without_backup_files(glob.glob('INSTALL-*'))

service_files = without_backup_files(glob.glob('*.service'))

not_config_files = set(['INSTALL.py', 'APT-PACKAGES', 'APACHE-MODULES', 'RUBY-GEMS', 'SERVICES'] + install_scripts + service_files)

if os.path.exists('APT-PACKAGES'):
    requested_packages = set(open('APT-PACKAGES').read().split())

    installed_packages = set()
    for line in subprocess_check('apt list --installed').split('\n'):
        installed_packages.add(line.split('/', 1)[0])

    needed_packages = sorted(list(requested_packages - installed_packages))

    if needed_packages:
        #subprocess_check('apt-get update', verbose=True)

        try:
            subprocess_check(['apt', '--yes', 'install', '--no-upgrade'] + needed_packages, verbose=True)
        except Exception as e:
            print('Caught exception %s' % e)

if os.path.exists('APACHE-MODULES'):    
    modules = open('APACHE-MODULES').read().split()
    subprocess_check(['a2enmod'] + modules, verbose=True)
    
if os.path.exists('RUBY-GEMS'):    
    modules = open('RUBY-GEMS').read().split()
    out = subprocess_check(['gem', 'install'] + modules, verbose=True)

for script in install_scripts:
    script = './' + script
    print('Running %s:' % script)
    print(subprocess_check([script]).strip())
    print('Finished %s' % script)

try:
    hostname = subprocess_check('hostname -f').strip()
except:
    # Omit -f flag to hostname if it fails
    hostname = subprocess_check('hostname').strip()

for src in srcs:
    if src in not_config_files or src[-1] == '~' or src[0] == '#' or os.path.isdir(src):
        continue
    src_abspath = os.path.abspath(src)

    file = open(src)
    firstline = file.readline().strip()
    if firstline[:3] == "#!/":
        firstline = file.readline().strip()
        
    tokens = shlex.split(firstline)[1:]

    if tokens and tokens[0] == 'HOSTMATCH':
        tokens.pop(0)
        match = tokens.pop(0)
        if not re.match(match, hostname):
            print('{src}: does not match hostname, skipping'.format(**locals()))
            continue
    if len(tokens) == 0:
        print('{src}:  first line missing'.format(**locals()))
        errs += 1
    elif tokens[0] == 'APPEND':
        if len(tokens) != 4 or tokens[2] != 'TO':
            print('{src}: usage APPEND "some line of text with optional <FILE>" TO filename'.format(**locals()))
            errs += 1
            continue
        line = tokens[1].replace('<FILE>', src_abspath)
        dest = os.path.expanduser(tokens[3])
        if not os.path.exists(dest):
            print('{src}: {dest} does not exist'.format(**locals()))
            errs += 1
            continue
        if line in [l.strip() for l in open(dest)]:
            continue
        print('Adding {line} to {dest}'.format(**locals()))
        open(dest, 'a').write('\n' + line + '\n')
    elif tokens[0] == 'SYMLINK':
        if len(tokens) != 2:
            print('{src}: usage SYMLINK dest-file-or-directory'.format(**locals()))
            errs += 1
            continue
        dest = os.path.expanduser(tokens[1])
        if os.path.isdir(dest):
            dest = os.path.join(dest, src)
        if os.path.exists(dest) and os.path.islink(dest) and os.readlink(dest) == src_abspath:
            # Already done
            continue
        if os.path.exists(dest):
            moveaside = dest + moveaside_suffix
            print('Moving aside {dest} to {moveaside}'.format(**locals()))
            os.rename(dest, moveaside)
        print('Symlinking {src_abspath} to {dest}'.format(**locals()))
        os.symlink(src_abspath, dest)   
    elif tokens[0] == 'IGNORE':
        continue
    else:
        print('{src}: Cannot understand first line {firstline}'.format(**locals()))
        errs += 1

absolute_service_files = list(map(os.path.abspath, service_files))

for service in absolute_service_files:
    print('%s: enabling and starting (if not already enabled and started)' % service)
    subprocess_check(['systemctl', 'enable', service], verbose=True)
    subprocess_check(['systemctl', 'start', os.path.basename(service)], verbose=True)
    
if os.path.exists('SERVICES'):
    for service in open('SERVICES'):
        service = service.strip()
        print('%s: enabling and starting (if not already enabled and started)' % service)
        subprocess_check(['systemctl', 'enable', service], verbose=True)
        subprocess_check(['systemctl', 'start', service], verbose=True)

if errs:
    print('Failed to complete successfully')
else:
    print('Done')

    
