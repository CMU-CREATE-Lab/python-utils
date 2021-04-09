#!/usr/bin/env python3

import AnacondaInstaller

import argparse, os

parser = argparse.ArgumentParser()
parser.add_argument('target_path', help='Target path (e.g. foo/anaconda3)')
parser.add_argument('--list', action='store_true', help='List all packages in installation (both conda and pip)')
parser.add_argument('--dry-run', action='store_true', help='List all packages in installation (both conda and pip)')
parser.add_argument('package_list_file', help='Package list file (default to <name>.list from python-utils)', nargs="?", default="auto")
args = parser.parse_args()

if args.package_list_file == 'auto':
    package_list_file = f"{os.path.dirname(os.path.abspath(__file__))}/{os.path.basename(args.target_path)}.list"
else:
    package_list_file = args.package_list_file

print(f"Using list file {package_list_file}")

if args.list:
    print(f'Currently installed packages in {args.target_path}:')
    print('\n'.join(sorted(AnacondaInstaller.currently_installed_packages(args.target_path))))
else:
    AnacondaInstaller.install(
        args.target_path, 
        conda_list_filename = package_list_file,
        dry_run = args.dry_run)
