"""
Functions to validate the input files prior to database insert / upload.
"""
import time

import numpy as np
import pandas as pd

import IEDC_paths
from IEDC_tools import dbio, file_io, __version__


def create_aspects_table(file_meta):
    """
    Pulls the info on classification and attributes together, i.e. make sense of the messy attributes in an actual
    table... More of a convenience function for tired programmers.
    See sheet 'Cover' in template file.
    :param file: Filename, string
    :return: Dataframe table with name, classification_id, and attribute_no
    """
    # Read the file and put metadata and row_classifications in two variables
    dataset_info = file_meta['dataset_info']
    row_classifications = file_meta['row_classifications']
    col_classifications = file_meta['col_classifications']
    # Filter relevant rows from the metadata table, i.e. the ones containing 'aspect'
    custom_aspects = dataset_info[dataset_info.index.str.startswith('aspect_')]
    custom_aspects = custom_aspects[custom_aspects.index.str.endswith('_classification')]
    # Get rid of the empty ones
    custom_aspects = custom_aspects[custom_aspects['Dataset entries'] != 'none']
    # Here comes the fun... Let's put everything into a dict, because that is easily converted to a dataframe
    d = {'classification_id': custom_aspects['Dataset entries'].values,
         'index': [i.replace('_classification', '') for i in custom_aspects.index],
         'name': dataset_info.loc[[i.replace('_classification', '')
                                   for i in custom_aspects.index]]['Dataset entries'].values}
    if file_meta['data_type'] == 'LIST':
        d['attribute_no'] = row_classifications.reindex(d['name'])['Aspects_Attribute_No'].values
        d['position'] = 'row?'
    elif file_meta['data_type'] == 'TABLE':
        d['attribute_no'] = row_classifications \
            .reindex(d['name'])['Row_Aspects_Attribute_No'] \
            .fillna(col_classifications.Col_Aspects_Attribute_No).values
        # The table format file has no info on the position of aspects. Need to find that.
        d['position'] = []
        for n in d['name']:
            if n in row_classifications.index:
                d['position'].append('row' + str(row_classifications.index.get_loc(n)))
            if n in col_classifications.index:
                d['position'].append('col' + str(col_classifications.index.get_loc(n)))
    assert not any([i is None for i in d['attribute_no']])  # 'not any' means 'none'
    # Convert to df and get rid of the redundant 'index' column
    aspect_table = pd.DataFrame(d, index=d['index']).drop('index', axis=1)
    return aspect_table


def get_class_names(file_meta, aspect_table):
    """
    Creates and looks up names for classification, i.e. classifications that are not found in the database (custom)
    will be generated and existing ones (non-custom) looked up in the classification_definitions table.
    The name is generated as a combination of the dataset name and the classification name, e.g.
    "1_F_steel_SankeyFlows_2008_Global_﻿origin_process".
    The function extends the table created in create_aspects_table() and returns it.
    :param file: Filename, string
    :return: Dataframe table with name, classification_id, attribute_no, and classification_definition
    """
    dataset_info = file_meta['dataset_info']
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    r = []
    for aspect in aspect_table.index:
        if aspect_table.loc[aspect, 'classification_id'] == 'custom':
            r.append(aspect_table.loc[aspect, 'name'] + '__' + dataset_info.loc['dataset_name', 'Dataset entries'])
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


def check_classification_items(class_names, file_meta, file_data, crash=True, warn=True,
                               custom_only=False, exclude_custom=False):
    """
    Checks in classification_items if a. all classification_ids exists and b. all attributes exist

    :param class_names: List of classification names
    :param file_data: Dataframe of Excel file, sheet `Data`
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
        # remove garbage from string
        try:
            attrib_no = attrib_no.strip(' ')
        except:
            pass
        if attrib_no != 'custom' and custom_only:
            continue  # skip already existing classifications
        if attrib_no == 'custom' and exclude_custom:
            continue  # skip custom classifications
        # make sure classification id exists -- must pass, otherwise the next command will fail
        assert class_names.loc[aspect, 'custom_name'] in db_classdef['classification_name'].values, \
            "Classification '%s' does not exist in table 'classification_definiton'" % \
            class_names.loc[aspect, 'custom_name']
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
            print(aspect, class_id)

        # Next check if all attributes exist
        if attrib_no == 'custom':
            attrib_no = 'attribute1_oto'
        else:
            attrib_no = 'attribute' + str(int(attrib_no)) + '_oto'
        checkme = db_classitems.loc[db_classitems['classification_id'] == class_id][attrib_no].values
        if file_meta['data_type'] == 'LIST':
            attributes = file_data[class_names.loc[aspect, 'name']].unique()
        elif file_meta['data_type'] == 'TABLE':
            if class_names.loc[aspect, 'position'][:3] == 'row':
                attributes = file_data.index.levels[int(class_names.loc[aspect, 'position'][-1])]
            elif class_names.loc[aspect, 'position'][:3] == 'col':
                attributes = file_data.columns.levels[int(class_names.loc[aspect, 'position'][-1])]
        for attribute in attributes:
            if str(attribute) in checkme:
                exists.append(True)
                if crash:
                    raise AssertionError("'%s' already in %s" % (attribute, checkme))
                elif warn:
                    print("WARNING: '%s' already in classification_items" % attribute)
            else:
                exists.append(False)
                print(aspect, attribute, class_id)
    return exists


def create_db_class_defs(file, file_meta):
    """
    Writes the custom classification to the table classification_definition.

    :param file: The data file to read.
    TODO: Discuss UNIQUE database constraint with Stefan
    """
    class_names = get_class_names(file, file_meta)
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
             'created_from_dataset': True,  # signifies that this is a custom classification
             'general': False,
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
    file_data = file_io.read_candidate_data_list(file)
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
        attributes = sorted(file_data[class_names.loc[aspect, 'name']].apply(str).unique())
        df = pd.DataFrame({'classification_id': [d['classification_id']] * len(attributes),
                           'description': [d['description']] * len(attributes),
                           'reference': [d['reference']] * len(attributes),
                           'attribute1_oto': attributes})
        columns = ('classification_id', 'description', 'reference', 'attribute1_oto')
        dbio.bulk_sql_insert('classification_items', columns, df.values.tolist())
        print("Wrote attributes for custom classification '%s' to classification_items" % class_id)


def add_user(file_meta, quiet=False):
    dataset_info = file_meta['dataset_info']
    db_user = dbio.get_sql_table_as_df('users')
    realname = dataset_info.loc['submitting_user'].values[0]
    if realname in db_user['name'].values:
        if not quiet:
            print("User '%s' already exists in db table users" % realname)
    else:
        d = {'name': realname,
             'username': (realname.split(' ')[0][0] + realname.split(' ')[1]).lower(),
             'start_date': time.strftime('%Y-%m-%d %H:%M:%S')
             }
        dbio.dict_sql_insert('users', d)
        print("User '%s' written to db table users" % d['username'])


def add_license(file_meta, quiet=False):
    dataset_info = file_meta['dataset_info']
    db_licenses = dbio.get_sql_table_as_df('licences')
    file_licence = dataset_info.loc['project_license'].values[0]
    if file_licence in db_licenses['name'].values:
        if not quiet:
            print("Licence '%s' already exists in db table 'licences'" % file_licence)
    else:
        d = {'name': file_licence,
             'description': 'n/a, generated by IEDC_tools v%s' % __version__}
        dbio.dict_sql_insert('licences', d)
        print("Licence '%s' written to db table 'licences'" % file_licence)


def parse_stats_array(stats_array_strings):
    """
    Parses the 'stats_array string' from the Excel template. E.g. "3;10;3.0;none;" should fill the respecitve columns
     in the data table as follows: stats_array_1 = 3, stats_array_2 = 10, stats_array_3 = 3.0, stats_array_4 = none
    More info: https://github.com/IndEcol/IE_data_commons/issues/14
    :param stats_array_strings:
    :return:
    """
    temp_list = []
    for sa_string in stats_array_strings:
        if sa_string == 'none':
            temp_list.append([None] * 4)
        else:
            assert len(sa_string.split(';')) == 4, "The 'stats_array string' is not well formatted: %s" % sa_string
            temp_list.append(sa_string.split(';'))
    return_df = pd.DataFrame(temp_list)
    return_df = return_df.replace(['none'], [None])
    # return a list of lists
    return [return_df[i].values for i in range(len(return_df.columns))]


def upload_data_list(file_meta, aspect_table, file_data, crash=True):
    """
    Uploads the actual data from the Excel template file (sheet Data) into the database.
    :param file: Name of the file to read. String.
    :param crash: Will stop if an error occurs
    :return:
    """
    class_names = get_class_names(file_meta, aspect_table)
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
    assert all(check_classification_items(class_names, file_meta, file_data,
                                          crash=False, custom_only=False, warn=False)),\
        "Not all classification_ids or attributes found in classification_items"
    # Time to loop through the file's aspects
    dataset_name = file_meta['dataset_info'].loc['dataset_name', 'Dataset entries']
    dataset_id = db_datasets[db_datasets['dataset_name'] == dataset_name].index.values[0]
    if crash:
        # TODO: Who creates the datasets table? IMO this should cause an error
        assert dataset_id not in db_datasets['dataset_name'], \
            "The database already contains values for dataset_id '%s' in the 'datasets' table" % dataset_id
    else:
        print("WARNING: The database already contains values for dataset_id '%s'" % dataset_id)
    assert dataset_id not in db_data_ids, \
        "The database already contains values for dataset_id '%s' in the 'data' table" % dataset_id
    # TODO: There is a bad mismatch between Excel templates and the db's data table. Ugly code ahead.
    more_df_columns = ['value', 'unit nominator', 'unit denominator', 'comment']
    more_sql_columns = ['value', 'unit_nominator', 'unit_denominator', 'stats_array_1', 'stats_array_2',
                        'stats_array_3', 'stats_array_4', 'comment']
    file_data['dataset_id'] = dataset_id
    df_columns = ['dataset_id'] + class_names['name'].values.tolist() + more_df_columns
    sql_columns = ['dataset_id'] + [a.replace('_','') for a in class_names.index] + more_sql_columns
    # sql_columns = [a + '_oto' if a.startswith('aspect') else a for a in sql_columns]
    data = file_data[df_columns]
    # Now for the super tedious replacement of names with ids...
    for n, aspect in enumerate(class_names.index):
        db_classitems2 = db_classitems[db_classitems['classification_id'] == class_ids[n]]
        attribute_no = class_names.loc[aspect, 'attribute_no']
        if attribute_no == 'custom':
            attribute_no = 1
        class_name = class_names.loc[aspect, 'name']
        file_data[class_name] = file_data[class_name].apply(str)
        tmp = file_data.merge(db_classitems2, left_on=class_name, right_on='attribute%s_oto' %
                                                                               str(int(attribute_no)), how='left')
        # TODO: Causes annoying warning in Pandas. Not sure if relevant: https://stackoverflow.com/q/20625582/2075003
        data[class_name] = tmp['i']

    for nom_denom in ('unit nominator', 'unit denominator'):
        # check if all units present in one of the units columns
        for unit in file_data[nom_denom].unique():
            if str(unit) in db_units['unitcode'].values:
                merge_col = 'unitcode'
            elif str(unit) in db_units['alt_unitcode'].values:
                merge_col = 'alt_unitcode'
            elif str(unit) in db_units['alt_unitcode2'].values:
                merge_col = 'alt_unitcode2'
            else:
                raise AssertionError("The following unit is not in units table: %s" %
                                     set(file_data[nom_denom].unique()).difference(db_units['unitcode'].values))
        file_data[nom_denom] = file_data[nom_denom].apply(str)
        tmp = file_data.merge(db_units, left_on=nom_denom, right_on=merge_col, how='left')
        assert not any(tmp['id'].isnull()), "The following units do not exist in the units table: %s" % \
                                            file_data[tmp['id'].isnull()][nom_denom].unique()
        # TODO: Causes annoying warning in Pandas. Not sure if relevant: https://stackoverflow.com/q/20625582/2075003
        data[nom_denom] = tmp['id']
    # parse the stats_array_string column
    [data.insert(len(data.columns)-1, 'stats_array_%s' % str(n+1), l) for n, l in
     enumerate(parse_stats_array(file_data['stats_array string']))]
    # data['stats_array_1'], data['stats_array_2'], data['stats_array_3'], data['stats_array_4'] = \
    #     parse_stats_array(file_data['stats_array string'])
    # clean up some more mess
    data = data.replace(['none'], [None])
    data = data.replace([np.nan], [None])
    # look up values in classification_items
    dbio.bulk_sql_insert('data', sql_columns, data.values.tolist())
    print("Wrote data for '%s', dataset_id: %s" % (dataset_name, dataset_id))


def upload_data_table(file_meta, aspect_table, file_data, crash=True):
    """
    Uploads the actual data from the Excel template file (sheet Data) into the database.
    :param file: Name of the file to read. String.
    :param crash: Will stop if an error occurs
    :return:
    """
    class_names = get_class_names(file_meta, aspect_table)
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
    assert all(check_classification_items(class_names, file_meta, file_data,
                                          crash=False, custom_only=False, warn=False)),\
        "Not all classification_ids or attributes found in classification_items"
    # Time to loop through the file's aspects
    dataset_name = file_meta['dataset_info'].loc['dataset_name', 'Dataset entries']
    dataset_id = db_datasets[db_datasets['dataset_name'] == dataset_name].index.values[0]
    if crash:
        # TODO: Who creates the datasets table? IMO this should cause an error
        assert dataset_id not in db_datasets['dataset_name'], \
            "The database already contains values for dataset_id '%s' in the 'datasets' table" % dataset_id
    else:
        print("WARNING: The database already contains values for dataset_id '%s'" % dataset_id)
    assert dataset_id not in db_data_ids, \
        "The database already contains values for dataset_id '%s' in the 'data' table" % dataset_id
    # TODO: There is a bad mismatch between Excel templates and the db's data table. Ugly code ahead.
    more_df_columns = ['value']
    more_sql_columns = ['value', 'unit_nominator', 'unit_denominator', 'stats_array_1', 'stats_array_2',
                        'stats_array_3', 'stats_array_4', 'comment']
    file_data['dataset_id'] = dataset_id
    df_columns = ['dataset_id'] + class_names['name'].values.tolist() + more_df_columns
    sql_columns = ['dataset_id'] + [a.replace('_','') for a in class_names.index] + more_sql_columns
    # sql_columns = [a + '_oto' if a.startswith('aspect') else a for a in sql_columns]
    if file_meta['data_type'] == 'LIST':
        data = file_data[df_columns]
    elif file_meta['data_type'] == 'TABLE':
        data = file_data.melt([df_columns])
    # Now for the super tedious replacement of names with ids...
    for n, aspect in enumerate(class_names.index):
        db_classitems2 = db_classitems[db_classitems['classification_id'] == class_ids[n]]
        attribute_no = class_names.loc[aspect, 'attribute_no']
        if attribute_no == 'custom':
            attribute_no = 1
        class_name = class_names.loc[aspect, 'name']
        file_data[class_name] = file_data[class_name].apply(str)
        tmp = file_data.merge(db_classitems2, left_on=class_name, right_on='attribute%s_oto' %
                                                                               str(int(attribute_no)), how='left')
        # TODO: Causes annoying warning in Pandas. Not sure if relevant: https://stackoverflow.com/q/20625582/2075003
        data[class_name] = tmp['i']

    for nom_denom in ('unit nominator', 'unit denominator'):
        # check if all units present in one of the units columns
        for unit in file_data[nom_denom].unique():
            if str(unit) in db_units['unitcode'].values:
                merge_col = 'unitcode'
            elif str(unit) in db_units['alt_unitcode'].values:
                merge_col = 'alt_unitcode'
            elif str(unit) in db_units['alt_unitcode2'].values:
                merge_col = 'alt_unitcode2'
            else:
                raise AssertionError("The following unit is not in units table: %s" %
                                     set(file_data[nom_denom].unique()).difference(db_units['unitcode'].values))
        file_data[nom_denom] = file_data[nom_denom].apply(str)
        tmp = file_data.merge(db_units, left_on=nom_denom, right_on=merge_col, how='left')
        assert not any(tmp['id'].isnull()), "The following units do not exist in the units table: %s" % \
                                            file_data[tmp['id'].isnull()][nom_denom].unique()
        # TODO: Causes annoying warning in Pandas. Not sure if relevant: https://stackoverflow.com/q/20625582/2075003
        data[nom_denom] = tmp['id']
    # parse the stats_array_string column
    [data.insert(len(data.columns)-1, 'stats_array_%s' % str(n+1), l) for n, l in
     enumerate(parse_stats_array(file_data['stats_array string']))]
    # data['stats_array_1'], data['stats_array_2'], data['stats_array_3'], data['stats_array_4'] = \
    #     parse_stats_array(file_data['stats_array string'])
    # clean up some more mess
    data = data.replace(['none'], [None])
    data = data.replace([np.nan], [None])
    # look up values in classification_items
    dbio.bulk_sql_insert('data', sql_columns, data.values.tolist())
    print("Wrote data for '%s', dataset_id: %s" % (dataset_name, dataset_id))








