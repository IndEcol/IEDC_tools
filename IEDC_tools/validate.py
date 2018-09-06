"""
Functions to validate the input files prior to database insert / upload.
"""
import numpy as np
import pandas as pd

from IEDC_tools import dbio, file_io, __version__


def valid_aspect_class(file):
    """
    Checks if all aspects that are specified in the data template file are actually defined in the database (aspects
    table).
    :param file: Candidate data file name. String.
    :return: None. Does only assert.
    """
    meta = file_io.read_candidate_meta(file)
    file_aspects2 = meta['classifications'].index
    db_aspects = dbio.get_sql_table_as_df('aspects')['aspect'].values
    for aspect in file_aspects2:
        assert aspect in db_aspects, "The aspect '%s' was not found in the database." % (aspect,)


def valid_attribute(file):
    """
    Checks if all attributes of a non-custom classification exist in the database (table classification_items).
    :return:
    """
    file_data = file_io.read_candidate_data(file)
    aspect_table = get_aspects_table(file)
    aspect_table = aspect_table[aspect_table['classification_id'] != 'custom']
    for aspect in aspect_table.index:
        # Get the number of the aspect from dataset information metadata table
        aspect_no = aspect.split('_')[-1]
        # This is the classification id value for the 'aspect_x_classification' key in dataset information.
        # It will be used to look up the values from the classification_items table.
        classification_id = aspect_table.loc[aspect, 'classification_id']
        # Get the attribute number to select the right column (e.g. attribute2) from the classification_items table
        attrib_no = aspect_table.loc[aspect, 'attribute_no']
        attrib_name = aspect_table.loc[aspect, 'name']
        # Get all possible values from the classification_items table.
        # For better performance this could be taken out of the loop and looked up from a dataframe.
        db_attributes = dbio.get_sql_table_as_df('classification_items',
                                                 addSQL="WHERE classification_id = %s" % classification_id)
        # Select the candidates from above's query
        checkme = db_attributes["attribute" + str(attrib_no)].values
        # Now go through all unique items in the database file and see if it is contained in
        #   the classification_items table.
        for attribute in file_data[attrib_name].unique():
            assert str(attribute) in checkme, "'%s' not in %s" % (attribute, checkme)


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
            r.append(db_classdef.loc[aspect_table.loc[aspect, 'classification_id'], 'classification_Name'])
    aspect_table['custom_name'] = r
    return aspect_table


def create_db_class_defs(file, crash=True):
    """
    Writes the custom classification to the table classification_definition.
    :param file: The data file to read.
    # TODO: Discuss UNIQUE database constraint with Stefan
    :param crash: Strongly recommended -- will cause the script to stop if the classification already exists. Otherwise
        there could be ambiguous classifications with multiple IDs.
    """
    class_names = get_class_names(file)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    db_aspects = dbio.get_sql_table_as_df('aspects', index='aspect')
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        if class_names.loc[aspect, 'custom_name'] in db_classdef['classification_Name'].values:
            if crash:
                raise AssertionError("'%s' already exists in the DB classification table." %
                                     class_names.loc[aspect, 'custom_name'])
            print("Warning: '%s' already exists in the DB classification table. It will be overwritten." %
                  class_names.loc[aspect, 'custom_name'])
        else:
            d = {'classification_Name': str(class_names.loc[aspect, 'custom_name']),
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


@dbio.db_cursor_write
def create_db_class_items(curs, file, crash=True):
    """
    Writes the unique database items / attributes of a custom classification to the database.
    :param curs: Database cursor. The decorator takes care of this
    :param file: Data file to read
    :param crash: Strongly recommended -- will cause the script to stop if the classification_id already exists in
        classification_items. Otherwise there could be ambiguous values with multiple IDs.
    """
    class_names = get_class_names(file)
    file_data = file_io.read_candidate_data(file)
    db_classdef = dbio.get_sql_table_as_df('classification_definition')
    db_classitems = dbio.get_sql_table_as_df('classification_items')
    for aspect in class_names.index:
        if class_names.loc[aspect, 'classification_id'] != 'custom':
            continue  # skip already existing classifications
        # get classification_id
        class_id = db_classdef.loc[db_classdef['classification_Name'] ==
                                   class_names.loc[aspect, 'custom_name']].index[0]
        if class_id in db_classitems['classification_id'].unique():
            if crash:
                raise AssertionError("classification_id '%s' already exists in the table classification_items." %
                                     class_id)
            print("WARNING: classification_id '%s' already exists in the table classification_items."
                  "Proceeding - this will likely lead to redundant values!" %
                                 class_id)
        d = {'classification_id': class_id,
             'description': 'generated by IEDC_tools v%s' % __version__,
             'reference': class_names.loc[aspect, 'custom_name'].split('__')[1]}
        attributes = np.sort(file_data[class_names.loc[aspect, 'name']].unique())
        df = pd.DataFrame({'classification_id': [d['classification_id']] * len(attributes),
                           'description': [d['description']] * len(attributes),
                           'reference': [d['reference']] * len(attributes),
                           'attribute1': attributes})
        curs.executemany("""INSERT INTO iedc_review.classification_items
                                -- TODO: Discuss with Stefan if data should not rather go in a custom attribute column 
                                (classification_id, description, reference, attribute1)
                            VALUES (%s, %s, %s, %s);""",
                         df.values.tolist())
        print("Wrote custom classification '%s' to classification_items" % class_id)
