"""
Functions to validate the input files prior to database insert / upload.
"""
import time

import numpy as np
import pandas as pd

from IEDC_tools import dbio, file_io, __version__


def get_aspects_table(file):
    """
    Pulls the info on classification and attributes together, i.e. make sense of the messy attributes in an actual
    table... More of a convenience function for tired programmers.
    See sheet 'Cover' in template file.
    :param file: Filename, string
    :return: Dataframe table with name, classification_id, and attribute_no
    """
    # Read the file and put metadata and classifications in two variables
    file_meta = file_io.read_candidate_meta(file)
    meta = file_meta['meta']
    classifications = file_meta['classifications']
    # Filter relevant rows from the metadata table, i.e. the ones containing 'aspect'
    custom_aspects = meta[meta.index.str.startswith('aspect_')]
    custom_aspects = custom_aspects[custom_aspects.index.str.endswith('_classification')]
    # Get rid of the empty ones
    custom_aspects = custom_aspects[custom_aspects['Dataset entries'] != 'none']
    # Here comes the fun... Let's put everything into a dict, because that is easily converted to a dataframe
    d = {'classification_id': custom_aspects['Dataset entries'].values,
         'index': [i.replace('_classification', '') for i in custom_aspects.index],
         'name': meta.loc[[i.replace('_classification', '') for i in custom_aspects.index]]['Dataset entries'].values,
         }
    d['attribute_no'] = classifications.loc[d['name']]['Aspects_Attribute_No'].values
    # Convert to df and get rid of the redundant 'index' column
    aspect_table = pd.DataFrame(d, index=d['index']).drop('index', axis=1)
    return aspect_table


def get_class_names(file):
    """
    Creates and looks up names for classification, i.e. classifications that are not found in the database (custom)
    will be generated and existing ones (non-custom) looked up in the classification_definitions table.
    The name is generated as a combination of the dataset name and the classification name, e.g.
    "1_F_steel_SankeyFlows_2008_Global_﻿origin_process".
    The function extends the table created in get_aspects_table() and returns it.
    :param file: Filename, string
    :return: Dataframe table with name, classification_id, attribute_no, and classification_definition
    """
    meta = file_io.read_candidate_meta(file)['meta']
    aspect_table = get_aspects_table(file)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    r = []
    for aspect in aspect_table.index:
        if aspect_table.loc[aspect, 'classification_id'] == 'custom':
            r.append(aspect_table.loc[aspect, 'name'] + '__' + meta.loc['dataset_name', 'Dataset entries'])
        else:
            r.append(db_classdef.loc[aspect_table.loc[aspect, 'classification_id'], 'classification_name'])
    aspect_table['custom_name'] = r
    return aspect_table


def check_classification_definition(class_names, crash=True, warn=True,
                                    custom_only=False, exclude_custom=False):
    """
    Checks if classifications exists in the database, i.e. classification_definition.

    :param class_names: List of classification names
    :param crash: Strongly recommended -- will cause the script to stop if the classification already exists. Otherwise
    there could be ambiguous classifications with multiple IDs.
    :param warn: Allows to suppress the warning message
    :param custom_only: Check only custom classifications
    :param exclude_custom: Exclude custom classifications
    :return: True or False
    """
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    exists = []
    for aspect in class_names.index:
        attrib_no = class_names.loc[aspect, 'attribute_no']
        if attrib_no != 'custom' and custom_only:
            continue  # skip already existing classifications
        if attrib_no == 'custom' and exclude_custom:
            continue  # skip custom classifications
        if class_names.loc[aspect, 'custom_name'] in db_classdef['classification_name'].values:
            exists.append(True)
            if crash:
                raise AssertionError("'%s' already exists in the DB classification table." %
                                     class_names.loc[aspect, 'custom_name'])
            elif warn:
                print("WARNING: '%s' already exists in the DB classification table. "
                      "Adding it again may fail or create ambiguous values." %
                      class_names.loc[aspect, 'custom_name'])
        else:
            exists.append(False)
    return exists


def check_classification_items(class_names, file_data, crash=True, warn=True,
                               custom_only=False, exclude_custom=False):
    """
    Checks in classification_items if a. all classification_ids exists and b. all attributes exist

    :param class_names: List of classification names
    :param file_data: Dataframe of Excel file, sheet `Values_Master`
    :param crash: Strongly recommended -- will cause the script to stop if the classification_id already exists in
        classification_items. Otherwise there could be ambiguous values with multiple IDs.
    :param custom_only: Check only custom classifications
    :param exclude_custom: Exclude custom classifications
    :param warn: Allows to suppress the warning message
    :return:
    """
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    db_classitems = dbio.get_sql_table_as_df('classification_items')
    exists = []  # True / False switch
    for aspect in class_names.index:
        attrib_no = class_names.loc[aspect, 'attribute_no']
        if attrib_no != 'custom' and custom_only:
            continue  # skip already existing classifications
        if attrib_no == 'custom' and exclude_custom:
            continue  # skip custom classifications
        # get classification_id
        class_id = db_classdef.loc[db_classdef['classification_name'] ==
                                   class_names.loc[aspect, 'custom_name']].index[0]
        # Check if the classification_id already exists in classification_items
        if class_id in db_classitems['classification_id'].unique():
            exists.append(True)
            if crash:
                raise AssertionError("classification_id '%s' already exists in the table classification_items." %
                                     class_id)
            elif warn:
                print("WARNING: classification_id '%s' already exists in the table classification_items. "
                      "Adding its attributes again may fail or create ambiguous values." %
                      class_id)
        else:
            exists.append(False)

        # Next check if all attributes exist
        attributes = file_data[class_names.loc[aspect, 'name']].unique()
        if attrib_no == 'custom':
            attrib_no = 'attribute1'
        else:
            attrib_no = 'attribute' + str(attrib_no)
        checkme = db_classitems.loc[db_classitems['classification_id'] == class_id][attrib_no].values
        for attribute in attributes:
            if str(attribute) in checkme:
                exists.append(True)
                if crash:
                    raise AssertionError("'%s' already in %s" % (attribute, checkme))
                elif warn:
                    print("WARNING: '%s' already in classification_items" % attribute)
            else:
                exists.append(False)
    return exists


def create_db_class_defs(file):
    """
    Writes the custom classification to the table classification_definition.

    :param file: The data file to read.
    TODO: Discuss UNIQUE database constraint with Stefan
    """
    class_names = get_class_names(file)
    db_aspects = dbio.get_sql_table_as_df('aspects', index='aspect')
    check_classification_definition(class_names, custom_only=True)
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        d = {'classification_name': str(class_names.loc[aspect, 'custom_name']),
             'dimension': str(db_aspects.loc[class_names.loc[aspect, 'name'], 'dimension']),
             'description': 'generated by IEDC_tools v%s' % __version__,
             'mutually_exclusive': True,
             'collectively_exhaustive': False,
             # TODO: rename column reserve5 to custom
             'reserve5': True,  # signifies that this is a custom classification
             'meaning_attribute1': "none"  # cannot be NULL???
             }
        dbio.dict_sql_insert('classification_definition', d)
        print("Wrote custom classification '%s' to classification_definitions" %
              class_names.loc[aspect, 'custom_name'])


def create_db_class_items(file):
    """
    Writes the unique database items / attributes of a custom classification to the database.

    :param file: Data file to read
    """
    class_names = get_class_names(file)
    file_data = file_io.read_candidate_data(file)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    check_classification_items(class_names, file_data, custom_only=True, crash=True)
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        # get classification_id
        class_id = db_classdef.loc[db_classdef['classification_name'] ==
                                   class_names.loc[aspect, 'custom_name']].index[0]
        d = {'classification_id': class_id,
             'description': 'generated by IEDC_tools v%s' % __version__,
             'reference': class_names.loc[aspect, 'custom_name'].split('__')[1]}
        attributes = np.sort(file_data[class_names.loc[aspect, 'name']].unique())
        df = pd.DataFrame({'classification_id': [d['classification_id']] * len(attributes),
                           'description': [d['description']] * len(attributes),
                           'reference': [d['reference']] * len(attributes),
                           'attribute1': attributes})
        columns = ('classification_id', 'description', 'reference', 'attribute1')
        dbio.bulk_sql_insert('classification_items', columns, df.values.tolist())
        print("Wrote attributes for custom classification '%s' to classification_items" % class_id)


def add_user(file):
    meta = file_io.read_candidate_meta(file)['meta']
    db_user = dbio.get_sql_table_as_df('users')
    realname = meta.loc['submitting_user'].values[0]
    if realname in db_user['name'].values:
        print("User '%s' already exists in db table users" % realname)
    else:
        d = {'name': realname,
             'username': (realname.split(' ')[0][0] + realname.split(' ')[1]).lower(),
             'start_date': time.strftime('%Y-%m-%d %H:%M:%S')
             }
        dbio.dict_sql_insert('users', d)
        print("User '%s' written to db table users" % d['username'])


def add_license(file):
    meta = file_io.read_candidate_meta(file)['meta']
    db_licenses = dbio.get_sql_table_as_df('licences')
    file_licence = meta.loc['project_license'].values[0]
    if file_licence in db_licenses['name'].values:
        print("Licence '%s' already exists in db table 'licences'" % file_licence)
    else:
        d = {'name': file_licence,
             'description': 'n/a, generated by IEDC_tools v%s' % __version__}
        dbio.dict_sql_insert('licences', d)
        print("Licence '%s' written to db table 'licences'" % file_licence)


def upload_data(file, crash=True):
    """
    Uploads the actual data from the Excel template file (sheet Values_Master) into the database.
    :param file: Name of the file to read. String.
    :param crash: Will stop if an error occurs
    :return:
    """
    class_names = get_class_names(file)
    meta = file_io.read_candidate_meta(file)
    file_data = file_io.read_candidate_data(file)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    db_units = dbio.get_sql_table_as_df('units', index=None)
    class_ids = [db_classdef[db_classdef['classification_name'] == i].index[0] for i
                 in class_names['custom_name'].values]
    db_classitems = dbio.get_sql_table_as_df('classification_items', addSQL="WHERE classification_id IN (%s)" %
                                                                            ', '.join([str(s) for s in class_ids]))
    db_classitems['i'] = db_classitems.index
    db_datasets = dbio.get_sql_table_as_df('datasets')
    db_data_ids = dbio.get_sql_table_as_df('data', ['DISTINCT dataset_id'], index=None).values
    # Let's make sure all classifications and attributes exist in the database
    assert all(check_classification_definition(class_names, crash=False, custom_only=False, warn=False)), \
        "Not all classifications found in classification_definitions"
    assert all(check_classification_items(class_names, file_data, crash=False, custom_only=False, warn=False)),\
        "Not all classification_ids or attributes found in classification_items"
    # Time to loop through the file's aspects
    dataset_name = meta['meta'].loc['dataset_name', 'Dataset entries']
    dataset_id = db_datasets[db_datasets['dataset_name'] == dataset_name].index.values[0]
    if crash:
        # TODO: Who creates the datasets table? IMO this should cause an error
        assert dataset_id not in db_datasets['dataset_name'], \
            "The database already contains values for dataset_id '%s' in the 'datasets' table" % dataset_id
        assert dataset_id not in db_data_ids, \
            "The database already contains values for dataset_id '%s' in the 'data' table" % dataset_id
    else:
        print("WARNING: The database already contains values for dataset_id '%s'" % dataset_id)
    # TODO: There is a bad mismatch between Excel templates and the db's data table. Ugly code ahead.
    more_df_columns = ['value', 'unit nominator', 'unit denominator', 'stats_array string', 'comment']
    more_sql_columns = ['value', 'unit_nominator', 'unit_denominator', 'stats_array_1', 'comment']
    file_data['dataset_id'] = dataset_id
    df_columns = ['dataset_id'] + class_names['name'].values.tolist() + more_df_columns
    sql_columns = ['dataset_id'] + [a.replace('_','') for a in class_names.index] + more_sql_columns
    data = file_data[df_columns]
    # Now for the super tedious replacement of names with ids...
    for n, aspect in enumerate(class_names.index):
        pass
        db_classitems2 = db_classitems[db_classitems['classification_id'] == class_ids[n]]
        attribute_no = class_names.loc[aspect, 'attribute_no']
        if attribute_no == 'custom':
            attribute_no = 1
        class_name = class_names.loc[aspect, 'name']
        try:
            tmp = file_data.merge(db_classitems2, left_on=class_name, right_on='attribute%s' % attribute_no,
                                  how='outer')
        except ValueError:
            file_data[class_name] = file_data[class_name].apply(str)
            tmp = file_data.merge(db_classitems2, left_on=class_name, right_on='attribute%s' % attribute_no,
                                  how='outer')
        data[class_name] = tmp['i']
    for u in ('unit nominator', 'unit denominator'):
        tmp = file_data.merge(db_units, left_on=u, right_on='unitcode',
                              how='outer')
        data[u] = tmp['id']
    # clean up some more mess
    data = data.replace(['none'], [None])
    # TODO: Megatons missing from units table. Someone needs to fix
    # look up values in classification_items
    test = data.values.tolist()
    dbio.bulk_sql_insert('data', sql_columns, data.values.tolist())
    print("Wrote data for '%s'" % dataset_name)








