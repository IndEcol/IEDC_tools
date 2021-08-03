"""
Functions to validate the input files prior to database insert / upload.
"""
import time

import numpy as np
import pandas as pd

import IEDC_paths, IEDC_pass
from IEDC_tools import dbio, file_io, __version__


def check_datasets_entry(file_meta, create=True, crash_on_exist=True, update=True, replace=False):
    """
    Creates an entry in the `datasets` table.
    :param file_meta: data file metadata
    :param crash_on_exist: if True: function terminates with assertion error if dataset/version already exists
    :param update: if True: function updates dataset entry if dataset/version already exists
    :param create: if True: funtion creates dataset entry for dataset/version
    :param replace: if True: delete existing entry in dataset table and create new one with current data
    """
    db_datasets = dbio.get_sql_table_as_df('datasets')
    dataset_info = file_meta['dataset_info']
    # Check if entry already exists
    dataset_name_ver = [i[0] for i in dataset_info.loc[['dataset_name', 'dataset_version']]
                        .where((pd.notnull(dataset_info.loc[['dataset_name', 'dataset_version']])), None).values]
    if dataset_name_ver[1] in ['NULL']:
        dataset_name_ver[1] = None
    # If exists already
    if dataset_name_ver in db_datasets[['dataset_name', 'dataset_version']].values.tolist(): # dataset name + verion already exists in dataset catalog
        if crash_on_exist:
            raise AssertionError("Database already contains the following dataset (dataset_name, dataset_version):\n %s"
                                 % dataset_name_ver)
        elif update:
            update_dataset_entry(file_meta)
        elif replace:
            # get id
            if dataset_name_ver[1] == None:
                db_id = db_datasets.loc[(db_datasets['dataset_name'] == dataset_name_ver[0]) &
                                        pd.isna(db_datasets['dataset_version'])].index[0]
            else:
                db_id = db_datasets.loc[(db_datasets['dataset_name'] == dataset_name_ver[0]) &
                                        (db_datasets['dataset_version'] == dataset_name_ver[1])].index[0]
            dbio.run_this_command("DELETE FROM %s.datasets WHERE id = %s;" % (IEDC_pass.IEDC_database, db_id))
            # add new one
            create_dataset_entry(file_meta)
        else:
            # do nothing
            print("Database already contains the following dataset (dataset_name, dataset_version):\n %s"
                  % dataset_name_ver)
            return True
    # if it doesn't exist yet
    else:
        if create:
            create_dataset_entry(file_meta)
        else:  # i.e. crash_on_not_exist
            raise AssertionError("Database does not contain the following dataset (dataset_name, dataset_version):\n %s"
                                 % dataset_name_ver)


def create_dataset_entry(file_meta):
    dataset_info = file_meta['dataset_info']
    dataset_info = dataset_info.replace([np.nan], [None])
    dataset_info = dataset_info.replace({'na': None, 'nan': None, 'none': None,
                                         'NULL': None})
    dataset_info = dataset_info.to_dict()['Dataset entries']
    assert dataset_info['dataset_id'] == 'auto', \
        "Was hoping 'dataset_id' in the file template had the value 'auto'. Not sure what to do now..."
    # Clean up dict
    dataset_info.pop('dataset_id')
    if pd.isna(dataset_info['reserve5']):
        dataset_info['reserve5'] = 'Created by IEDC_tools v%s' % __version__
    # Look up stuff
    data_types = dbio.get_sql_table_as_df('types')
    dataset_info['data_type'] = data_types.loc[data_types['name'] == dataset_info['data_type']].index[0]
    data_layers = dbio.get_sql_table_as_df('layers')
    dataset_info['data_layer'] = data_layers.loc[data_layers['name'] == dataset_info['data_layer']].index[0]
    data_provenance = dbio.get_sql_table_as_df('provenance')
    dataset_info['data_provenance'] = data_provenance.loc[data_provenance['name'] ==
                                                          dataset_info['data_provenance']].index[0]
    aspects = dbio.get_sql_table_as_df('aspects')
    class_defs = dbio.get_sql_table_as_df('classification_definition')
    for aspect in [i for i in dataset_info.keys() if i.startswith('aspect_')]:
        if dataset_info[aspect] is None or aspect.endswith('classification'):
            continue
        if dataset_info[aspect+'_classification'] == 'custom':
            aspect_class_name = str(dataset_info[aspect]) + '__' + dataset_info['dataset_name']
            dataset_info[aspect+'_classification'] = \
                class_defs[class_defs['classification_name'] == aspect_class_name].index[0]
        dataset_info[aspect] = aspects[aspects['aspect'] == dataset_info[aspect]].index[0]
    source_type = dbio.get_sql_table_as_df('source_type')
    dataset_info['type_of_source'] = source_type.loc[source_type['name'] == dataset_info['type_of_source']].index[0]
    licenses = dbio.get_sql_table_as_df('licences')
    dataset_info['project_license'] = licenses.loc[licenses['name'] == dataset_info['project_license']].index[0]
    users = dbio.get_sql_table_as_df('users')
    dataset_info['submitting_user'] = users.loc[users['name'] == dataset_info['submitting_user']].index[0]
    # fix some more
    for k in dataset_info:
        # not sure why but pymysql doesn't like np.int64
        if type(dataset_info[k]) == np.int64:
            dataset_info[k] = int(dataset_info[k])
    dbio.dict_sql_insert('datasets', dataset_info)
    print("Created entry for %s in 'datasets' table." % [dataset_info[k] for k in ['dataset_name', 'dataset_version']])
    return None


def update_dataset_entry(file_meta):
    raise NotImplementedError


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
                raise AssertionError("""Classification '%s' already exists in the DB classification table (ID: %s). 
                Aspect '%s' cannot be processed.""" %
                                     (class_names.loc[aspect, 'custom_name'],
                                      db_classdef[db_classdef['classification_name']
                                                  == 'general_product_categories'].index[0], aspect))
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
            print(aspect, class_id, 'not in classification_items')

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
                if len(file_meta['row_classifications'].values) == 1:
                    attributes = file_data.index.values
                else:
                    attributes = file_data.index.levels[int(class_names.loc[aspect, 'position'][-1])]
            elif class_names.loc[aspect, 'position'][:3] == 'col':
                if len(file_meta['col_classifications'].values) == 1:
                    # That means there is only one column level defined, i.e. no MultiIndex
                    attributes = file_data.columns.values
                else:
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
                print(aspect, attribute, class_id, 'not in classification_items')
    return exists


def create_db_class_defs(file_meta, aspect_table):
    """
    Writes the custom classification to the table classification_definition.

    :param file: The data file to read.
    """
    class_names = get_class_names(file_meta, aspect_table)
    db_aspects = dbio.get_sql_table_as_df('aspects', index='aspect')
    check_classification_definition(class_names, custom_only=True)
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        d = {'classification_name': str(class_names.loc[aspect, 'custom_name']),
             'dimension': str(db_aspects.loc[class_names.loc[aspect, 'name'], 'dimension']),
             'description': 'Custom classification, generated by IEDC_tools v%s' % __version__,
             'mutually_exclusive': True,
             'collectively_exhaustive': False,
             'created_from_dataset': True,  # signifies that this is a custom classification
             'general': False,
             'meaning_attribute1': "'%s' aspect of dataset" % aspect  # cannot be NULL???
             }
        dbio.dict_sql_insert('classification_definition', d)
        print("Wrote custom classification '%s' to classification_definitions" %
              class_names.loc[aspect, 'custom_name'])


def create_db_class_items(file_meta, aspects_table, file_data):
    """
    Writes the unique database items / attributes of a custom classification to the database.

    :param file: Data file to read
    """
    class_names = get_class_names(file_meta, aspects_table)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    check_classification_items(class_names, file_meta, file_data, custom_only=True, crash=True)
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        # get classification_id
        class_id = db_classdef.loc[db_classdef['classification_name'] ==
                                   class_names.loc[aspect, 'custom_name']].index[0]
        d = {'classification_id': class_id,
             'description': 'Custom classification, generated by IEDC_tools v%s' % __version__,
             'reference': class_names.loc[aspect, 'custom_name'].split('__')[1]}
        if file_meta['data_type'] == 'LIST':
            attributes = sorted(file_data[class_names.loc[aspect, 'name']].apply(str).unique())
        elif file_meta['data_type'] == 'TABLE':
            if class_names.loc[aspect, 'position'][:-1] == 'col':
                if len(file_meta['col_classifications'].values) == 1:
                    # That means there is only one column level defined, i.e. no MultiIndex
                    attributes = [str(i) for i in file_data.columns]
                else:
                    attributes = sorted(
                        [str(i) for i in file_data.columns.levels[int(class_names.loc[aspect, 'position'][-1])]])
            elif class_names.loc[aspect, 'position'][:-1] == 'row':
                if len(file_meta['row_classifications'].values) == 1:
                    attributes = [str(i) for i in file_data.index]
                else:
                    attributes = sorted(
                        [str(i) for i in file_data.index.levels[int(class_names.loc[aspect, 'position'][-1])]])
        df = pd.DataFrame({'classification_id': [d['classification_id']] * len(attributes),
                           'description': [d['description']] * len(attributes),
                           'reference': [d['reference']] * len(attributes),
                           'attribute1_oto': attributes})
        columns = ('classification_id', 'description', 'reference', 'attribute1_oto')
        dbio.bulk_sql_insert('classification_items', columns, df.values.tolist())
        print("Wrote attributes for custom classification '%s' to classification_items: %s" % (class_id, attributes))


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


def parse_stats_array_list(stats_array_strings):
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


def parse_stats_array_table(file, file_meta, row_indices, col_indices):
    # db_sa = dbio.get_sql_table_as_df('stats_array', index=None)
    if file_meta['data_sources'].loc['Dataset_Uncertainty', 'a'] == 'GLOBAL':
        if file_meta['data_sources'].loc['Dataset_Uncertainty', 'b'] in ('none', 'None'):
            sa_res = [None] * 4
        else:
            sa_res = file_meta['data_sources'].loc['Dataset_Uncertainty', 'b'].split(';')
        return {'type': 'GLOBAL',
                'data': sa_res}
    elif file_meta['data_sources'].loc['Dataset_Uncertainty', 'a'] == 'TABLE':
        file_sa = file_io.read_stats_array_table(file, row_indices, col_indices)
        sa_tmp = file_sa.reset_index().melt(file_sa.index.names)
        sa_tmp = sa_tmp.set_index(row_indices)
        # parse the string https://stackoverflow.com/a/21032532/2075003
        sa_res = sa_tmp['value'].str.split(';', expand=True)
        sa_res.columns = ['stats_array_' + str(i+1) for i in range(4)]
        sa_res = sa_res.replace(['none'], [None])
        sa_res = sa_res.astype({'stats_array_1': int, 'stats_array_2': float,
                                'stats_array_3': float, 'stats_array_4': float})
        return {'type': 'TABLE',
                'data': sa_res}
    else:
        raise AttributeError("Unknown data unit type specified. Must be either 'GLOBAL' or 'TABLE'.")


def get_comment_table(file, file_meta, row_indices, col_indices):
    if file_meta['data_sources'].loc['Dataset_Comment', 'a'] == 'GLOBAL':
        if file_meta['data_sources'].loc['Dataset_Comment', 'b'] in ('none', 'None'):
            comment = None
        else:
            comment = file_meta['data_sources'].loc['Dataset_Comment', 'b']
        return {'type': 'GLOBAL',
                'data': comment}
    elif file_meta['data_sources'].loc['Dataset_Comment', 'a'] == 'TABLE':
        comment = file_io.read_comment_table(file, row_indices, col_indices)
        comment = comment.reset_index().melt(comment.index.names)
        comment = comment.set_index(row_indices)
        return {'type': 'TABLE',
                'data': comment}
    else:
        raise AttributeError("Unknown data unit type specified. Must be either 'GLOBAL' or 'TABLE'.")


def get_unit_list(file_data):
    db_units = dbio.get_sql_table_as_df('units', index=None)
    res = pd.DataFrame()
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
        if len(tmp.index) != len(file_data.index):
            raise AssertionError("Duplicate entry on (unit nominator,unit denominator) tuple in the unit table. Data upload haltet. Check unit table!")
        assert not any(tmp['id'].isnull()), "The following units do not exist in the units table: %s" % \
                                            file_data[tmp['id'].isnull()][nom_denom].unique()
        # TODO: Causes annoying warning in Pandas. Not sure if relevant: https://stackoverflow.com/q/20625582/2075003
        res[nom_denom] = tmp['id']
    return res


def upload_data_list(file_meta, aspect_table, file_data, crash=True):
    """
    Uploads the actual data from the Excel template file (sheet Data) into the database.
    :param file: Name of the file to read. String.
    :param crash: Will stop if an error occurs
    :return:
    """
    class_names = get_class_names(file_meta, aspect_table)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
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
    assert dataset_name in db_datasets['dataset_name'].values, \
        "'%s' does not exist in the 'datasets' table" % dataset_name
    dataset_id = db_datasets[db_datasets['dataset_name'] == dataset_name].index.values[0]
    if crash:
        # TODO: Who creates the datasets table? IMO this should cause an error
        assert dataset_id not in db_datasets['dataset_name'], \
            "The database already contains values for dataset_id '%s' in the 'datasets' table" % dataset_id
    else:
        print("WARNING: The database already contains values for dataset_id '%s' in the 'datasets' table" % dataset_id)
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
        if len(tmp.index) != len(data.index):
            raise AssertionError("The database classification table contains at least one conflicting duplicate entry for the unique attribute attribute%s_oto of classification %s. Data upload halted. Check classification for duplicate entries!" % (str(int(attribute_no)), class_name))        
        data.loc[:, class_name] = tmp['i']
    units = get_unit_list(file_data)
    data['unit nominator'] = units['unit nominator']
    data['unit denominator'] = units['unit denominator']
    # parse the stats_array_string column
    [data.insert(len(data.columns)-1, 'stats_array_%s' % str(n+1), l) for n, l in
     enumerate(parse_stats_array_list(file_data['stats_array string']))]
    # data['stats_array_1'], data['stats_array_2'], data['stats_array_3'], data['stats_array_4'] = \
    #     parse_stats_array_list(file_data['stats_array string'])
    # clean up some more mess
    data = data.replace(['none'], [None])
    data = data.replace([np.nan], [None])
    # look up values in classification_items
    dbio.bulk_sql_insert('data', sql_columns, data.values.tolist())
    print("Wrote data for '%s', dataset_id: %s" % (dataset_name, dataset_id))


def get_unit_table(file, file_meta, row_indices, col_indices):
    db_units = dbio.get_sql_table_as_df('units', index=None)
    # first method for LIST type data and also for certain TABLE type
    if file_meta['data_sources'].loc['Dataset_Unit', 'a'] == 'GLOBAL':
        merge_col = {}
        for nom_denom in ('u_nominator', 'u_denominator'):
            if str(file_meta[nom_denom]) == '1.0':
                file_meta[nom_denom] = '1'
            # check if all units present in one of the units columns
            if str(file_meta[nom_denom]) in db_units['unitcode'].values:
                merge_col[nom_denom] =  'unitcode'
            elif str(file_meta[nom_denom]) in db_units['alt_unitcode'].values:
                merge_col[nom_denom] = 'alt_unitcode'
            elif str(file_meta[nom_denom]) in db_units['alt_unitcode2'].values:
                merge_col[nom_denom] = 'alt_unitcode2'
            else:
                raise AssertionError("The following unit is not in units table: %s" % file_meta[nom_denom])
            file_meta[nom_denom] = str(file_meta[nom_denom])
        return {'type': 'GLOBAL',
                'nominator': int(db_units.loc[db_units[merge_col['u_nominator']] == file_meta['u_nominator']]['id']),
                'denominator': int(db_units.loc[db_units[merge_col['u_denominator']] ==
                                                file_meta['u_denominator']]['id'])}
    elif file_meta['data_sources'].loc['Dataset_Unit', 'a'] == 'TABLE':
        file_units = file_io.read_units_table(file, row_indices, col_indices)
        units = {}
        for nom_denom in file_units:
            units[nom_denom] = file_units[nom_denom].reset_index().melt(file_units[nom_denom].index.names)
            units[nom_denom] = units[nom_denom].set_index(row_indices)
            for u in units[nom_denom]['value'].unique():
                # check if all units present in one of the units columns
                if str(u) in db_units['unitcode'].values:
                    merge_col = 'unitcode'
                elif str(u) in db_units['alt_unitcode'].values:
                    merge_col = 'alt_unitcode'
                elif str(u) in db_units['alt_unitcode2'].values:
                    merge_col = 'alt_unitcode2'
                else:
                    raise AssertionError("The following unit is not in units table: %s" % u)
                units[nom_denom].loc[units[nom_denom]['value'] == u, 'icol'] = \
                    int(db_units.loc[db_units[merge_col] == str(u)]['id'])
            # TODO: Remove
            #  res = pd.DataFrame(index=ordered_index.set_index(row_indices).index)
            #  res['nominator'] = units[nom_denom]['value']
            #  res['icol'] = units['icol']
        return {'type': 'TABLE',
                'nominator': units['Unit_nominator'],
                'denominator': units['Unit_denominator']}
    else:
        raise AttributeError("Unknown data unit type specified. Must be either 'GLOBAL' or 'TABLE'.")


def upload_data_table(file, file_meta, aspect_table, file_data, crash=True):
    """
    Uploads the actual data from the Excel template file (sheet Data) into the database.
    Dataset entry must already be present in dataset table, use validate.check_datasets_entry to ensure that.
    Main table data must not contain any values for this dataset id (unique for dataset name and version).
    :param file: Name of the file to read. String.
    :param crash: Will stop if an error occurs
    :return:
    """
    class_names = get_class_names(file_meta, aspect_table)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
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
                                          crash=False, custom_only=False, warn=False)), \
        "Not all classification_ids or attributes found in classification_items"
    dataset_info = file_meta['dataset_info']
    # Check if entry already exists
    dataset_name_ver = [i[0] for i in dataset_info.loc[['dataset_name', 'dataset_version']]
                        .where((pd.notnull(dataset_info.loc[['dataset_name', 'dataset_version']])), None).values]
    if dataset_name_ver[1] in ['NULL']:
        dataset_name_ver[1] = None
    # If the dataset name+version entry does not exist yet
    if dataset_name_ver not in db_datasets[['dataset_name', 'dataset_version']].values.tolist(): # dataset name + verion already exists in dataset catalog
        raise AssertionError("Database catalog does not contain the following dataset (dataset_name, dataset_version). Please use validate.check_datasets_entry to ensure that the catalog entry exists before uploading data for: %s"
                                 % dataset_name_ver)
    dataset_name = dataset_name_ver[0] # file_meta['dataset_info'].loc['dataset_name',    'Dataset entries']
    dataset_vers = dataset_name_ver[1] # file_meta['dataset_info'].loc['dataset_version', 'Dataset entries']
    # get id of dataset_name_ver:
    dataset_id = db_datasets[(db_datasets['dataset_name'] == dataset_name) & (db_datasets['dataset_version'] == dataset_vers)].index.values[0]
    # Check that no data are present already in the data table:
    if dataset_id in db_data_ids:
         raise AssertionError("The database already contains values for dataset_id '%s' in the 'data' table. This upload is cancelled to avoid conflicts." % dataset_id)
    # Gotta love Pandas: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.melt.html
    # https://stackoverflow.com/q/53464475/2075003
    data = file_data.reset_index().melt(file_data.index.names)
    data.insert(0, 'dataset_id', dataset_id)
    # Now for the super tedious replacement of names with ids...
    for n, aspect in enumerate(class_names.index):
        db_classitems2 = db_classitems[db_classitems['classification_id'] == class_ids[n]]
        attribute_no = class_names.loc[aspect, 'attribute_no']
        if attribute_no == 'custom':
            attribute_no = 1
        class_name = class_names.loc[aspect, 'name']
        if class_names.loc[aspect, 'position'][:3] == 'col':
            if len(file_meta['col_classifications'].values) == 1:
                # That means there is only one column level defined, i.e. no MultiIndex
                file_data.columns = [str(c) for c in file_data.columns]
            else:
                file_data.columns.set_levels(
                    [str(i) for i in file_data.columns.levels[file_data.columns.names.index(class_name)]],
                    level=file_data.columns.names.index(class_name), inplace=True)
        elif class_names.loc[aspect, 'position'][:3] == 'row':
            if len(file_meta['row_classifications'].values) == 1:
                file_data = file_data.rename({c: str(c) for c in file_data.index})
            else:
                file_data.index.set_levels(
                    [str(i) for i in file_data.index.levels[file_data.index.names.index(class_name)]],
                    level=file_data.index.names.index(class_name), inplace=True)
        # Make sure the columns that will be used to match have the same data type
        data[class_name] = data[class_name].astype(str)
        tmp = data.merge(db_classitems2, left_on=class_name,
                         right_on='attribute%s_oto' % str(int(attribute_no)), how='left')
        if len(tmp.index) != len(data.index):
            raise AssertionError("The database classification table contains at least one conflicting duplicate entry for the unique attribute attribute%s_oto of classification %s. Data upload halted. Check classification for duplicate entries!" % (str(int(attribute_no)), class_name))
        assert not any(pd.isna(tmp['i'])), "The correct classification could not be found for '%s'" % class_name         
        data.loc[:, class_name] = tmp['i']
    units = get_unit_table(file, file_meta, file_data.index.names, file_data.columns.names)
    if units['type'] == 'TABLE':
        data['unit_nominator'] = units['nominator']['icol'].apply(int).values
        data['unit_denominator'] = units['denominator']['icol'].apply(int).values
    elif units['type'] == 'GLOBAL':
        data['unit_nominator'] = units['nominator']
        data['unit_denominator'] = units['denominator']
    # parse the stats_array_string column
    stats_array = parse_stats_array_table(file, file_meta, file_data.index.names, file_data.columns.names)
    if stats_array['type'] == 'TABLE':
        data = pd.concat([data, stats_array['data'].reset_index(drop=True, inplace=True)], axis=1)
        for c in stats_array['data']:
            data[c] = stats_array['data'][c].values
    elif stats_array['type'] == 'GLOBAL':
        for n, c in enumerate(['stats_array_' + str(i+1) for i in range(4)]):
            data[c] = stats_array['data'][n]
        # [data.insert(len(data.columns) - 1, 'stats_array_%s' % str(n + 1), l) for n, l in
        #  enumerate(stats_array['data'])]
    comment = get_comment_table(file, file_meta, file_data.index.names, file_data.columns.names)
    if comment['type'] == 'GLOBAL':
        data['comment'] = comment['data']
    elif comment['type'] == 'TABLE':
        data['comment'] = comment['data']['value'].values
    # Seems to be a bug!  https://github.com/pandas-dev/pandas/issues/16784
    #  data = data.replace(['none'], [None])
    data = data.replace([np.nan], [None])
    for r in ['na', 'nan']:
        data = data.replace(r, None)
    # Not all classifications have this field yet...
    if 'Insert_Empty_Cells_as_NULL' in file_meta['data_sources'].index:
        # Check if NULL values should be skipped or added  https://github.com/IndEcol/IE_data_commons/issues/21
        if file_meta['data_sources'].loc['Insert_Empty_Cells_as_NULL', 'a'] == 'False':
            # No entry for empty data points
            print("`Insert_Empty_Cells_as_NULL` is set to False. Skipping %i empty / NULL values." % len(data[data['value'].isna()]))
            data = data[data['value'].notna()]
    # Get column names and order right
    more_sql_columns = ['value', 'unit_nominator', 'unit_denominator', 'stats_array_1', 'stats_array_2',
                        'stats_array_3', 'stats_array_4', 'comment']
    data = data[['dataset_id'] + class_names['name'].to_list() + more_sql_columns]
    sql_columns = ['dataset_id'] + [a.replace('_', '') for a in class_names.index] + more_sql_columns
    # look up values in classification_items
    dbio.bulk_sql_insert('data', sql_columns, data.values.tolist())
    print("Wrote data for '%s', dataset_id: %s" % (dataset_name, dataset_id))

