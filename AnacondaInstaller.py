import os, re, requests
import utils.utils as utils

def installer_date(installer):
    matches = re.findall(r'(\d\d\d\d\.\d\d)', installer)
    return matches and matches[0]

def installer_url():
    platform = utils.subprocess_check('uname -a').split()[0]
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

def install(dest_dir, conda_forge_packages=[], pip_packages=[]):
    installation_path = dest_dir + '/anaconda3'

    if os.path.exists(installation_path):
        print(f'{installation_path} already installed, skipping')
    else:
        installer_path = dest_dir + '/conda_installer.sh'

        utils.download_file(installer_url(), installer_path)
        utils.subprocess_check(['sh', installer_path, '-b', '-p', installation_path], verbose=True)
        os.unlink(installer_path)

    ### Install pip and conda packages

    use_anaconda = f". {installation_path}/bin/activate && conda activate {installation_path}"

    if conda_forge_packages:
        utils.subprocess_check(
            f"{use_anaconda} && conda install -y -c conda-forge {' '.join(conda_forge_packages)}",
            verbose=True
        )

    if pip_packages:
        utils.subprocess_check(f"{use_anaconda} && pip {' '.join(pip_packages)}", verbose=True)
