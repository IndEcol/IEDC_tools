from tqdm import tqdm

from IEDC_tools import file_io, validate
from IEDC_tools import dbio
import IEDC_paths

candidate_file = 'test.xlsx'


# print(inputs.read_input_file(IEDC_paths.MasterClassification))

# print(file_io.get_candidate_filenames(IEDC_paths.candidates, verbose=1))


# print(dbio.get_sql_table_as_df('aspects')['aspect'])

#validate.aspect_in_db(candidate_file)

#print(inputs.read_candidate_data_list(candidate_file)['origin_process'].unique())

#print(dbio.get_sql_table_as_df('classification_items', addSQL="WHERE classification_id = %s" % 1)["attribute" + str(3)].values)

#validate.aspect_in_db(candidate_file, verbose=True)
#validate.valid_attribute(candidate_file)

#print(validate.create_aspects_table(candidate_file))

#validate.get_class_names(candidate_file)

#validate.check_db_classification("origin_process__1_F_steel_SankeyFlows_2008_Global")

# validate.get_class_names(candidate_file)
# validate.create_db_class_defs(candidate_file)
# validate.create_db_class_items(candidate_file)
# validate.check_classification_definition(validate.get_class_names(candidate_file),
#                                          crash=False, custom_only=False)
# validate.check_classification_items(validate.get_class_names(candidate_file),
#                                     file_io.read_candidate_data_list(candidate_file),
#                                     crash=False, custom_only=False)
# validate.upload_data_list(candidate_file, crash=False)
#dbio.dict_sql_insert(1)
#validate.add_license(candidate_file)
#validate.add_user(candidate_file)


#class_names = validate.get_class_names(file)
#file_data = file_io.read_candidate_data_list(file)
#validate.check_classification_definition(class_names, crash=False)
# validate.create_db_class_defs(file)
# validate.create_db_class_items(file)
#print(validate.check_classification_items(class_names, file_data, crash=False))
#validate.create_db_class_items(file)



exclude_files = [
    '6_MIP_YSTAFDB_MetalUseShares_v1.0.xlsx',  # Done
    '4_PY_YSTAFDB_EoL_RecoveryRate_v1.0.xlsx',  # Done
    # '6_URB_MetabolismOfCities_Jan2019_DOI_7326485.v1.xlsx'  # TODO
    ]
path = IEDC_paths.candidates
focus = []

for file in tqdm(file_io.get_candidate_filenames(path, verbose=1)):
    if file in exclude_files:
        print("Skipping %s" % file)
        continue
    if len(focus) > 0 and file not in focus:
        continue
    try:
        file_meta = file_io.read_candidate_meta(file, path=path)
        aspects_table = validate.create_aspects_table(file_meta)
        class_names = validate.get_class_names(file_meta, aspects_table)
        file_data = file_io.read_candidate_data_list(file, path)
        if not all(validate.check_classification_definition(class_names, crash=False, warn=False)):
            validate.create_db_class_defs(file_meta, aspects_table)
        if not all(validate.check_classification_items(class_names, file_meta, file_data, crash=False, warn=False)):
            validate.create_db_class_items(file_meta, aspects_table, file_data)
        validate.add_user(file_meta, quiet=True)
        validate.add_license(file_meta, quiet=True)
        validate.check_datasets_entry(file_meta, crash_on_exist=False, create=True, update=False, replace=True)
        validate.upload_data_list(file_meta, aspects_table, file_data, crash=False)
    except BaseException as e:
        print("ERROR: File '%s' caused an issue. See stack." % file)
        raise e

