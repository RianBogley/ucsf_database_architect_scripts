# Data Importing Functions
# Import Packages
import os
import pandas as pd

# Import Input CSV File
def import_input_csv(input_csv):
    """
    Import Input CSV File
    """
    df = pd.read_csv(input_csv)
    # Check if df has PIDN column, otherwise return error and stop script:
    if 'PIDN' not in df.columns:
        print('Input CSV must have PIDN column. Please fix and try again.')
    # Check if df has DCDate column, if not continue with just PIDN:
    if 'PIDN' in df.columns and 'DCDate' not in df.columns:
        print('Input CSV has PIDN column but does not have a DCDate column. Continuing with just PIDN.')
    # Check if df has PIDN and DCDate columns, if so continue with both and check for missing values, if so return error:
    if 'PIDN' and 'DCDate' in df.columns:
        print('Input CSV has PIDN and DCDate columns. Continuing with both.')
        # Check if PIDN or DCDate columns have any missing values, if so return error:
        if df['PIDN'].isnull().values.any() or df['DCDate'].isnull().values.any():
            print('PIDN and/or DCDate columns have missing values. Please fix and try again.')
        # Convert DCDate column into Date Values leaving only YYYY-MM-DD format and convert NaN to NaT:
        df['DCDate'] = pd.to_datetime(df['DCDate'])
        # Sort df by DCDate
        df.sort_values('DCDate', inplace=True)
        # Create a copy of df['DCDate'] column and name it DCDate_input
        df['DCDate_input'] = df['DCDate']
    return df

# Import LAVA Query File
def import_lava_query(lava_queries_dir, type):
    """
    Import LAVA Query Data

    Some possible types include:
        demographics
        diagnosis
        ni_all
        bedside
        mmse
        language
        cdr
    """
    df = pd.read_excel(find_files(lava_queries_dir,type), sheet_name=0)
    # If no file is found, return error:
    if df.empty:
        print('No file found for type: ' + type)
    return df

# Import LAVA Data Dictionary
def import_lava_dict(lava_queries_dir):
    """
    Import LAVA Data Dictionary
    """
    df = pd.read_excel(find_files(lava_queries_dir,'lava_data_dictionary'), sheet_name=0)
    # If no file is found, return error:
    if df.empty:
        print('No LAVA Data Dictionary file found. Please download the most up-to-date version from LAVA Query and put it in the lava_queries directory.')
    return df

# Find files in directory based on specicific string in filename.
def find_files(dir_name, string):
    """
    Find files in directory based on specicific string in filename.
    """
    for file in os.listdir(dir_name):
        if string in file:
            output = os.path.join(dir_name, file)
    return output