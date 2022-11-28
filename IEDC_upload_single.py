# -*- coding: utf-8 -*-
"""
Created on Sat Jul 31 20:01:51 2021

@author: spauliuk
"""
# From https://github.com/IndEcol/IEDC_tools/blob/master/doc/getting_started.ipynb

# Since this notebook is in a subfolder, we need to include the path

# import sys
# sys.path.insert(0,'..')
# import os
# from pathlib import Path
from IEDC_tools import file_io, validate

# The directory browsing is not yet fully implemented. 
# So far IEDC_tools works only on a single file basis.
# Therefore it is necessary to specify a file. The path is specified in `IEDC_paths.py`
import IEDC_paths

##Filenames_List: Use validate.upload_data_list to upload
#filen = '7_CT_ISO_Regions_to_RECC_v2.4_Regions.xlsx'

##Filenames_Table: Use validate.upload_data_table to upload
filen = 'Tbd.xlsx'

# Read file metadata (as dictionary)
file_meta = file_io.read_candidate_meta(filen, path=IEDC_paths.candidates)

# # Get the file's classifications
aspect_table = validate.create_aspects_table(file_meta)
class_names  = validate.get_class_names(file_meta,aspect_table)
class_names 

# Checks for the above classifications if they exist in the database,
#  i.e. classification_definition

validate.check_classification_definition(class_names, crash=False)

if file_meta['data_type'] == 'LIST':
    file_data = file_io.read_candidate_data_list(filen, path=IEDC_paths.candidates) # uses pd.read_excel
if file_meta['data_type'] == 'TABLE':
    file_data = file_io.read_candidate_data_table(filen, aspect_table, path=IEDC_paths.candidates)

file_data.head()

# Check if all classification items are present in the database, checks and reports each item, produces lengthy output.
# validate.check_classification_items(class_names, file_meta, file_data, crash=False)

# Check if dataset entry already exists, terminate if already exists.
# validate.check_datasets_entry(file_meta, create=False, crash_on_exist=True, update=False, replace=False)
    
# Finally it is time to upload the data to the `data` table
## For table-shaped templates:
#validate.upload_data_table(filen, file_meta, aspect_table, file_data, crash=True)
## For list-shaped templates:
#validate.upload_data_list(file_meta, aspect_table, file_data, crash=True)

#
#
#
#