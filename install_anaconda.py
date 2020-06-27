#!/usr/bin/env python3

import AnacondaInstaller

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('target_path', help='Target path (e.g. foo/anaconda3)')
parser.add_argument('conda_list_file', help='Output from conda list, to use for package names to install')
args = parser.parse_args()

AnacondaInstaller.install(
    args.target_path, 
    conda_list_filename = args.conda_list_file)
