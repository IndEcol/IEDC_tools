from tqdm import tqdm

from IEDC_tools import file_io, validate
from IEDC_tools import dbio
import IEDC_paths, IEDC_pass

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

exclude_files = [
    '6_CR_YSTAFDB_criticality_2019_v1.0.xlsx',  # Done
    '6_CR_YSTAFDB_criticality_ei_2019_v1.0.xlsx',  # Done
    '6_CR_YSTAFDB_criticality_sr_2019_v1.0.xlsx',  # Done
    '6_CR_YSTAFDB_criticality_vsr_2019_v1.0.xlsx',  # Done
    '3_LT_Wood_Carbon_MFA_Indonesia_Aryapratama_2019.xlsx',  # Done
    '1_F_Wood_Carbon_MFA_Indonesia_Aryapratama_2019.xlsx',  # Done
    '2_IUS_Wood_Carbon_MFA_Indonesia_Aryapratama_2019.xlsx',  # Done
    '1_F_UN_IRP_Global_Material_Flows_Database.xlsx'  # Done
                 ]
path = IEDC_paths.candidates
focus = []

print("Working on database '%s'" % IEDC_pass.IEDC_database)

for file in tqdm(file_io.get_candidate_filenames(path, verbose=1)):
    if file in exclude_files:
        print("Skipping %s" % file)
        continue
    if len(focus) > 0 and file not in focus:
        continue
    # print(file_io.read_candidate_meta(file))
    print(file)
    try:
        file_meta = file_io.read_candidate_meta(file, path=path)
        if file_io.ds_in_db(file_meta, crash=False):
            pass
        aspects_table = validate.create_aspects_table(file_meta)
        class_names = validate.get_class_names(file_meta, aspects_table)
        file_data = file_io.read_candidate_data_table(file, aspects_table, path)
        # validate.check_datasets_entry(file_meta)
        if not all(validate.check_classification_definition(class_names, crash=False, warn=False)):
            validate.create_db_class_defs(file_meta, aspects_table)
        if not all(validate.check_classification_items(class_names, file_meta, file_data, crash=False, warn=False)):
            validate.create_db_class_items(file_meta, aspects_table, file_data)
        validate.add_user(file_meta, quiet=True)
        validate.add_license(file_meta, quiet=True)
        validate.check_datasets_entry(file_meta, crash_on_exist=False, create=True, update=False, replace=True)
        validate.upload_data_table(file, file_meta, aspects_table, file_data, crash=False)
    except BaseException as e:
        print("ERROR: File '%s' caused an issue. See stack." % file)
        raise e


#class_names = validate.get_class_names(file)
#file_data = file_io.read_candidate_data_list(file)
#validate.check_classification_definition(class_names, crash=False)
# validate.create_db_class_defs(file)
# validate.create_db_class_items(file)
#print(validate.check_classification_items(class_names, file_data, crash=False))
#validate.create_db_class_items(file)
