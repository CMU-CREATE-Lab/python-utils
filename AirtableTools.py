# This is to support API access to the Airtable workspace created by Anne Wright for 
# covid response.
# Documentation of Airtable class at
#   https://airtable-python-wrapper.readthedocs.io/en/master/api.html
#
# Installation instructions at
#   https://github.com/gtalarico/airtable-python-wrapper
#     pip install airtable-python-wrapper
#
# Example notebook at
#   https://github.com/gtalarico/airtable-python-wrapper/blob/master/Airtable.ipynb

# This was cloned from cocalc-src/airtable_tools.ipynb on 10/23/2020 by Anne Wright
from sqlitedict import SqliteDict
from airtable import Airtable
from collections import defaultdict
import os
# Define a token to help with dependency checking
_airtable_tools_version_ = 2

##############################################
### AIRTABLE_KEY 

# Get your API key by going to https://airtable.com/account
# Look in the <> API section, then click in the purple
# box with dots to copy out the API key.

# By default, the system will look for a file called airtable_key.txt
# you can override the file by setting airtable_key_path 
# before including this file

# The airtable_config_map arg has primary key is base name, and secondary keys:
#   base_key
#   tables

# An example of what this would look like is like this:
# airtable_config_map = {
#         'purr_petition':{
#             'base_key':'applK8JGIG94ZXozX',
#             'table_names':['Petition responses']
#         },
#         'maskpatrol':{
#             'base_key':'appuEuhgDiR5GtOlR',
#             'table_names':['Cameras']
#         }
#     }

# Find base_key for an Airtable by going to the HELP menu in the upper right corner, 
# selecting API documentation, and finding the line that looks like:
#   The ID of this base is appXXXXXXXXXXXX.

class AirtableWrapper:
    def __init__(self, airtable_config_map, airtable_key_path=None):
        failed = False
        fail_msg = None

        try:
            airtable_key_path
        except:
            airtable_key_path = None

        if not airtable_key_path:
            airtable_key_path = "airtable_key.txt"

        if not os.path.exists(airtable_key_path):
            fail_msg = f"ERROR: Please, put Airtable API key in {airtable_key_path}, or set airtable_key_path to point to a file containing your Airtable API key"
            failed = True
        else:
            print(f"Read Airtable base key from {airtable_key_path}")
            with open(airtable_key_path,'r') as file:
                self.AIRTABLE_KEY = file.read().strip()
                assert len(self.AIRTABLE_KEY) == 17 and 'key' in self.AIRTABLE_KEY, f"Found malformed Airtable API key in {airtable_key_path}: '{self.AIRTABLE_KEY}'; should be of the form 'keyXXXXXXXXXXXXXX'"

        try:
            airtable_config_map
            if not isinstance(airtable_config_map,dict):
                fail_msg = f"ERROR: airtable_config_map needs to be a dict"
                failed = True

            for k,v in airtable_config_map.items():
                if not isinstance(v,dict):
                    fail_msg = f"ERROR: airtable_config_map[{k}] needs to be a dict"
                    failed = True
                if not 'base_key':
                    fail_msg = f"ERROR: airtable_config_map[{k}] needs to have a 'base_key' key"
                    failed = True
                if not 'table_names':
                    fail_msg = f"ERROR: airtable_config_map[{k}] needs to have a 'table_names' key"
                    failed = True
                elif not isinstance(v['table_names'],list):
                    fail_msg = f"ERROR: airtable_config_map[{k}]['table_names'] needs to be a list"
                    failed = True
        except Exception as e:
            print(f"ERROR: airtable_config_map not defined: {e}")
            failed = True    

        if not failed:
            self.airtable_config_map = airtable_config_map
            self.airtable_cache = defaultdict(lambda:{})
        else:
            assert False, fail_msg

    def get_table(self, base_name, table_name):
        try:
            self.airtable_config_map
            self.AIRTABLE_KEY
        except:
            print(f"ERROR: airtable_config_map missing")
            return None
    
        assert base_name in self.airtable_config_map, 'airtable_get_table: No such base %r in airtable_config_map.  Update in airtable_tools.ipynb and call update_airtable_config_table_from_map()'%(base_name)
        assert table_name in self.airtable_config_map[base_name]['table_names'], 'airtable_config_map: No such table %r in base %r in airtable_config_table.  Update in airtable_tools.ipynb and call update_airtable_config_table_from_map()'%(table_name,base_name)

        if table_name in self.airtable_cache[base_name]:
            return self.airtable_cache[base_name][table_name]
        
        base_key = self.airtable_config_map[base_name]['base_key']
        table = Airtable(base_key, table_name, api_key=self.AIRTABLE_KEY)

        # Cache for next time
        self.airtable_cache[base_name][table_name]=table

        return table