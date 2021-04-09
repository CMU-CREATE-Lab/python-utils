# %%

import itertools, os, re, requests, subprocess
import utils
from collections import defaultdict

# Notes
#
# As of Jul 16, anaconda install followed by conda install mamba -c conda-forge was wedging forever
# So instead, starting from Miniconda per the mamba recommendation
# curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh > foo.sh
# bash foo.sh
# anaconda3/bin/activate 
# conda install mamba -c conda-forge
# mamba install conda -c conda-forge

def installer_date(installer):
    matches = re.findall(r'(\d\d\d\d\.\d\d)', installer)
    return matches and matches[0]

def installer_url():
    platform = utils.subprocess_check('uname -a').split()[0]
    arch = None
    if platform == 'Darwin':
        platform = 'MacOSX'
    if platform == 'Linux':
        arch = 'x86_64'

    installer_dir = 'https://repo.anaconda.com/archive/'
    body = requests.get(installer_dir).text

    installers = re.findall(r'<a[^>]*?href="(.*?)"', body)

    matching_installers = []

    for installer in installers:
        tokens = re.split(r'[-\.]', installer)
        if ('Anaconda3' in tokens and 
                platform in tokens and
                (not arch or arch in tokens) and
                tokens[-1] == 'sh' and
                installer_date(installer)):
            matching_installers.append(installer)

    installer = max(matching_installers, key=installer_date)
    return installer_dir + installer

def parse_conda_list(conda_list, force_conda_forge = False):
    ret = defaultdict(lambda:[])
    for line in conda_list.split('\n'):
        if line and line[0] != '#':
            tokens = line.strip().split()
            name = tokens[0]
            if len(tokens) > 1:
                channel = tokens[-1]
            else:
                channel = 'pypi'
            if force_conda_forge and channel == 'conda':
                channel = 'conda-forge'
            ret[channel].append(name)
    return dict(ret)

def use_anaconda_prefix(installation_path):
    return f". {installation_path}/bin/activate && conda activate {installation_path}"

def run_and_parse_conda_list(installation_path):
    current_package_list = utils.subprocess_check(
            f'{use_anaconda_prefix(installation_path)} && conda list', 
            executable='/bin/bash')
    return parse_conda_list(current_package_list)

def package_names(conda_packages):
    return sorted(set(itertools.chain.from_iterable(conda_packages.values())))

def test_installation(installation_path):
    print(f'Testing installation at {installation_path}')
    use_anaconda = use_anaconda_prefix(installation_path)
    uwsgi_path = os.path.join(installation_path, 'bin/uwsgi')
    installed = package_names(run_and_parse_conda_list(installation_path))
    if 'uwsgi' in installed:
        utils.subprocess_check([uwsgi_path, '--version'], verbose=True)
    if 'geopandas' in installed:
        utils.subprocess_check(
            f'{use_anaconda} && python -c "import geopandas"', verbose=True, executable='/bin/bash')


def install(installation_path, packages=None, conda_list_filename=None, dry_run=False, force_conda_forge=False):
    installation_path = os.path.abspath(installation_path)
    if conda_list_filename:
        assert not(packages)
        packages = parse_conda_list(
            open(conda_list_filename).read(), force_conda_forge=force_conda_forge)

    if os.path.exists(installation_path):
        print(f'{installation_path} already installed')
    else:
        installer_path = f'/tmp/conda_installer_{os.getpid()}.sh'

        cmd = ['sh', installer_path, '-b', '-p', installation_path]

        if dry_run:
            print(f'Download {installer_url()} to {installer_path}')
            print(f'Running {cmd}')
        else:
            utils.download_file(installer_url(), installer_path)
            cmd = ['sh', installer_path, '-b', '-p', installation_path]
            print(cmd)
            subprocess.call(cmd)
            os.unlink(installer_path)

    ### Install pip and conda packages
    if packages:
        use_anaconda = use_anaconda_prefix(installation_path)

        run_and_parse_conda_list(installation_path)

        installed = package_names(run_and_parse_conda_list(installation_path))
        have_mamba = ('mamba' in installed)
        already_installed = []
        for package in sorted(installed):
            if package in packages:
                del packages[package]
                already_installed.append(package)

        if already_installed:
            print(f'Already installed: {already_installed}')
        
        before = ['conda', 'conda-forge']
        after = ['pypi']

        install_channels = list(packages.keys())
        for channel in before+after:
            if channel in install_channels:
                install_channels.remove(channel)
        
        for channel in before + install_channels + after:
            to_install = sorted(set(packages.get(channel, [])) - set(installed))
            if to_install:
                print()
                cmd = f'{use_anaconda} && '
                if channel == "pypi":
                    cmd += ' pip install '
                else:
                    if not have_mamba:
                        mamba_cmd = f'{use_anaconda} && conda install -y mamba -c conda-forge'
                        print(mamba_cmd)
                        if not dry_run:
                            subprocess.call(mamba_cmd, shell=True, executable='/bin/bash')
                        have_mamba = true
                    cmd += ' mamba install -y '
                cmd += " ".join(to_install)
                if not channel in ['pypi', 'conda']:
                    cmd += f' -c {channel}'
                print(cmd)
                if not dry_run:
                    subprocess.call(cmd, shell=True, executable='/bin/bash')

    test_installation(installation_path)
#install('../anaconda3', conda_list_filename='../anaconda-2020-06-27.list', force_conda_forge=True)
#install('..')


# %%
