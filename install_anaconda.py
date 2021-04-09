#!/usr/bin/env python3

import AnacondaInstaller

import argparse, os

parser = argparse.ArgumentParser()
parser.add_argument('target_path', help='Target path (e.g. foo/anaconda3)')
parser.add_argument('conda_list_file', help='Output from conda list, to use for package names to install', nargs="?", default="auto")
args = parser.parse_args()

if args.conda_list_file == 'auto':
    conda_list_file = f"{os.path.dirname(os.path.abspath(__file__))}/{os.path.basename(args.target_path)}.list"
else:
    conda_list_file = args.conda_list_file

print(f"Using list file {conda_list_file}")

AnacondaInstaller.install(
    args.target_path, 
    conda_list_filename = conda_list_file)
