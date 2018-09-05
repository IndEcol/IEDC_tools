"""
Functions to validate the input files prior to database insert / upload.
"""

from IEDC_tools import dbio, inputs


def valid_aspect_class(file):
    """
    Checks if all aspects that are specified in the data template file are actually defined in the database (aspects
    table).
    :param file: The candidate data filename
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
    meta_dict = inputs.read_candidate_meta(file)
    classifications = meta_dict['classifications'].to_dict()['Aspects_Attribute_No']
    meta = meta_dict['meta']
    # Filter only the columns that start with 'aspect' from the dataset information metadata table
    file_aspects = meta[meta.index.str.startswith('aspect')]
    for aspect, attrib_name in file_aspects.to_dict()['Dataset entries'].items():
        if aspect.endswith("classification"):
            continue
        # Get the number of the aspect from dataset information metadata table
        aspect_no = aspect.split('_')[-1]
        # This is the classification id value for the 'aspect_x_classification' key in dataset information.
        # It will be used to look up the values from the classification_items table.
        classification_id = meta.loc["aspect_%s_classification" % aspect_no, 'Dataset entries']
        if classification_id not in ['custom', 'none']:
            # Get the attribute number to select the right column (e.g. attribute2) from the classification_items table
            attrib_no = classifications[attrib_name]
            # Get all possible values from the classification_items table.
            # For more performance this could be taken out of the loop and filtered in a dataframe.
            db_attributes = dbio.get_sql_as_df('classification_items',
                                               addSQL="WHERE classification_id = %s" % classification_id)
            # Select the candidates from above's query
            checkme = db_attributes["attribute" + str(attrib_no)].values
            # Now go through all unique items in the database file and see if it is contained in
            #   the classification_items table.
            for attribute in file_data[attrib_name].unique():
                assert str(attribute) in checkme, "'%s' not in %s" % (attribute, checkme)


# TODO add custom classifications
