"""
Functions to validate the input files prior to database insert / upload.
"""
import pandas as pd

from IEDC_tools import dbio, inputs


def valid_aspect_class(file):
    """
    Checks if all aspects that are specified in the data template file are actually defined in the database (aspects
    table).
    :param file: Candidate data file name. String.
    :return: None. Does only assert.
    """
    meta = inputs.read_candidate_meta(file)
    file_aspects2 = meta['classifications'].index
    db_aspects = dbio.get_sql_as_df('aspects')['aspect'].values
    for aspect in file_aspects2:
        assert aspect in db_aspects, "The aspect '%s' was not found in the database." % (aspect,)


def valid_attribute(file):
    """
    Checks if all attributes of a non-custom classification exist in the database (table classification_items).
    :return:
    """
    file_data = inputs.read_candidate_data(file)
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
        db_attributes = dbio.get_sql_as_df('classification_items',
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
    file_meta = inputs.read_candidate_meta(file)
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


# TODO add custom classifications
