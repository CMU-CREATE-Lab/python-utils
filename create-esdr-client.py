#!/usr/bin/python

import argparse

def exec_ipynb(url):
    import json, re, urllib2
    nb = (urllib2.urlopen(url) if re.match(r'https?:', url) else open(url)).read()
    exec '\n'.join([''.join(cell['input']) for cell in json.loads(nb)['worksheets'][0]['cells'] if cell['cell_type'] == 'code']) in globals()

exec_ipynb('esdr-library.ipynb')

parser = argparse.ArgumentParser()
parser.add_argument("clientName")
parser.add_argument("--username", default='EDIT ME')
parser.add_argument("--password", default='EDIT ME')
args = parser.parse_args()

dest = 'esdrAuth.json'

print args.clientName

Esdr.save_client(dest, args.clientName, args.username, args.password)


