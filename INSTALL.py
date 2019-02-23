#!/usr/bin/env python3

import datetime, glob, os, re, shlex, subprocess


# Change dir to dir of script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def without_backup_files(files):
    return [file for file in files if not file.endswith('~') and not file.startswith('#')]

srcs = without_backup_files(glob.glob('*'))
errs = 0

moveaside_suffix = '-moveaside=' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

install_scripts = without_backup_files(glob.glob('INSTALL-*'))

not_config_files = set(['INSTALL.py', 'APT-PACKAGES', 'APACHE-MODULES', 'RUBY-GEMS', 'SERVICES'] + install_scripts)

if os.path.exists('APT-PACKAGES'):
    packages = open('APT-PACKAGES').read().split()
    cmd = ['apt', '--yes', 'install', '--no-upgrade'] + packages
    print(' '.join(cmd))
    out = subprocess.check_output(cmd)
    print(out.decode('utf8'))

if os.path.exists('APACHE-MODULES'):    
    modules = open('APACHE-MODULES').read().split()
    out = subprocess.check_output(['a2enmod'] + modules)
    print(out.decode('utf8'))
    
if os.path.exists('RUBY-GEMS'):    
    modules = open('RUBY-GEMS').read().split()
    out = subprocess.check_output(['gem', 'install'] + modules)
    print(out.decode('utf8'))

for script in install_scripts:
    script = './' + script
    print('Running %s:' % script)
    print(subprocess.check_output([script]).decode('utf8').strip())
    print('Finished %s' % script)

hostname = subprocess.check_output(['hostname', '-f']).decode('utf8').strip()

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

if os.path.exists('SERVICES'):
    services = open('SERVICES').read().split()
    for service in services:
        print('%s: enabling and starting (if not already enabled and started)' % service)
        subprocess.check_output(['systemctl', 'enable', service])
        subprocess.check_output(['systemctl', 'start', service])
    
if errs:
    print('Failed to complete successfully')
else:
    print('Done')

    
