import os

import pandas as pd

import IEDC_paths


def read_input_file(file):
    """
    Function to load the master classification file and return its content as a DataFrame.

    :return: Master classification data as pandas dataframe
    """
    df = pd.read_excel(file.file, sheet_name=file.sheet, header=file.header, index_col=file.index)
    df = df.drop(file.drop_rows, axis=0)
    df = df.drop(file.drop_rows, axis=0)
    return df


def get_candidate_filenames(path=IEDC_paths.candidates):
    """
    Browses a directory and returns the filenames as a list.

    :param path: The directory of the files to be scanned
    :return: List of filenames
    """
    # Let's exclude temporary and hidden files
    # TODO: Will probably need more love for Windows...
    exclude_first_letter = ['.', '~']
    files = os.listdir(path)
    files = [f for f in files if not any([f.startswith(ex) for ex in exclude_first_letter])]
    return files


def read_candidate_meta(file, path=IEDC_paths.candidates):
    """
    Will read a candidate file and return its metadata.

    The hardcoding of `skiprows` and `usecols` is ugly, but necessary with the current data template. Something to
    consider for the next template.

    :param file: Filename of the file to process
    :param path: Path of the file
    :return: Dictionary of dataframes for metadata, classifications, and data
    """
    # make it a proper path
    file = os.path.join(path, file)
    meta = pd.read_excel(file, sheet_name='Cover', usecols='C:D',
                         skiprows=[0, 1], index_col="Column name")
    classifications = pd.read_excel(file, sheet_name='Cover', usecols='F:G',
                                    skiprows=[i for i in range(10)], index_col="Aspects_classifications").dropna()
    return {'meta': meta, 'classifications': classifications}


def read_candidate_data(file, path=IEDC_paths.candidates):
    """
    Will read a candidate file and return its data.

    :param file: Filename of the file to process
    :param path: Path of the file
    :return: Dictionary of dataframes for metadata, classifications, and data
    """
    # make it a proper path
    file = os.path.join(path, file)
    data = pd.read_excel(file, sheet_name='Values_Master')
    return data


def read_candidate_files(path=IEDC_paths.candidates):
    """
    Runs read_candidate_file() for all files in a directory
    :param path: directory of files to run the function for
    :return: TODO: List of dataframes?
    """
    for file in get_candidate_filenames(path):
        read_candidate_file(file, path)
    # TODO
    return None
