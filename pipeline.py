#%% LIBRARIES ###############################################################
# Libraries to import:
from nilearn.maskers import NiftiMasker
import numpy as np
import matplotlib.pyplot as plt
from nilearn.maskers import NiftiMasker
from nilearn.image import get_data
import pandas as pd
import seaborn as sns
import scipy.stats as stats
import openpyxl
import os
from sklearn.feature_selection import VarianceThreshold
from nilearn.decoding import DecoderRegressor
from nilearn import plotting
from nilearn.plotting import plot_stat_map, show
import nilearn.datasets
import pingouin as pg
from scripts.import_data import import_input_csv
from scripts.import_data import import_lava_query
from scripts.import_data import import_lava_dict
from scripts.filter_data import filter_ni_all
from scripts.filter_data import filter_t1
from scripts.filter_data import filter_other_cog
from scripts.filter_data import filter_demographics
from scripts.filter_data import filter_diagnosis
from scripts.filter_data import filter_lava
from scripts.filter_data import filter_timepoints
from scripts.merge_data import merge_data

#%% TO-DO & POSSIBLE ERRORS #################################################
# - Add error handling for missing LAVA datasets
# - Add error handling for duplicate LAVA datasets
# - Add error handling for missing LAVA 

#%% SPECIFY PATHS ###########################################################
# Path to directory with Raw LAVA Query Data Export files:
lava_queries_dir = 'C:/Users/rbogley/Box/math_cognition_team/papers/PPA_math/rian/lava_query/'
# Path to desired output directory:
output_dir = 'C:/Users/rbogley/Box/math_cognition_team/papers/PPA_math/rian/'
#%% SPECIFY WHICH LAVA DATASETS TO IMPORT ###################################
# All:
lava_datasets = ['demographics','diagnosis','ni_all','bedside','mmse','cdr','language','cvlt','dkefs','moca','cats','other_cog']
# Neuroimaging Only:
# lava_datasets = ['ni_all']
# Diagnosis Only:  
# lava_datasets = ['diagnosis']

#%% DEFINE INPUT METHOD #####################################################
# Input Methods: Specify which input method to use below:

# Input Method 1: PIDN-only CSV
input_method = 'PIDN'
# Input Method 2: PIDN & DCDates CSV
#input_method = 'PIDN-DCDate'
# Input Method 3: No PIDN or DCDate List, LAVA only
#input_method = 'LAVA'

# If using Input Method 1 or 2, specify the path to the input CSV file below:
#input_csv = 'C:/Users/rbogley/Box/math_cognition_team/papers/PPA_math/rian/input_test.csv'
input_csv = 'C:/Users/rbogley/Box/math_cognition_team/papers/PPA_math/rian/input_abr.csv'

# If using Input Method 1 or 3, specify the LAVA dataset to prioritize and timepoint preference:

# Dataset: Pick which LAVA dataset to use as the input dataset for merging all other LAVA datasets to.
dataset = 'bedside'
# dataset = 'ni_all'

# Timepoint: Uncomment one of the following methods to use as your input timepoint for the specified dataset.
timepoint = 'first' # Earliest avaialble timepoint in dataset above
# timepoint = 'latest' # Latest available timepoint in dataset above
# timepoint = 'fullest' # Timepoint with the most data available in dataset above

#%% IMPORT INPUT DATASET ####################################################
# Specify which data to import as df_input based on input_method:
if input_method == 'PIDN-DCDate':
    # Import the specified input CSV with PIDNs & DCDates as df_input:
    df_input = import_input_csv(input_csv)
elif input_method == 'PIDN':
    # Import the specified input CSV with PIDNs only as df_input:
    df_input = import_input_csv(input_csv)
    # Find the PIDN column number and add a new empty DCDate column after it:
    pidn_col = df_input.columns.get_loc('PIDN')
    df_input.insert(pidn_col+1, 'DCDate', '')
    # Make a temporary dataframe importing data from the specified LAVA dataset and timepoint filter:
    df_temp = filter_timepoints(import_lava_query(lava_queries_dir, dataset), timepoint)
    # Add a DCDate column to the right of the PIDN column in df_input and put the DCDates from the temporary dataframe into the df_input dataframe matching by PIDN:
    df_input['DCDate'] = df_input['PIDN'].map(df_temp.set_index('PIDN')['DCDate'])
    # Drop any rows with missing DCDates and print the number of rows dropped:
    df_input = df_input.dropna(subset=['DCDate'])
    print('Dropped ' + str(len(df_input) - len(df_input.dropna(subset=['DCDate']))) + ' rows with missing DCDates.')
elif input_method == 'LAVA':
    # If using LAVA dataset method, import the specified LAVA dataset as df_input:
    df_input = filter_timepoints(df_lava[dataset], timepoint)
    # Drop all columns other than PIDN and DCDate:
    df_input = df_input[['PIDN','DCDate']]

#%% IMPORT LAVA DATASETS ####################################################
# Import all LAVA datasets specified in lava_datasets list:
df_lava = {}
for df in lava_datasets:
    df_lava[df] = import_lava_query(lava_queries_dir, df)
    # Filter LAVA data:
    if df == 'ni_all':
        # filter the ni_all dataframe
        df_lava[df] = filter_ni_all(df_lava[df])
        # create a copy of df 'ni_all' and name it 't1':
        df_lava['t1'] = df_lava[df].copy()
        # filter, sort, and trim the t1 dataframe
        df_lava['t1'] = filter_t1(df_lava['t1'])
        # remove the 'ni_all' df from df_lava
        df_lava.pop('ni_all')
    elif df == 'demographics':
        df_lava[df] = filter_demographics(df_lava[df])
    elif df == 'diagnosis':
         df_lava[df] = filter_diagnosis(df_lava[df])
    elif df == 'other_cog':
        df_lava[df] = filter_other_cog(df_lava[df])
    # else if df is anything else on lava_datasets list, use filter_lava on df_lava[df]:
    elif df == 'bedside':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'mmse':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'cdr':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'language':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'cvlt':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'dkefs':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'moca':
        df_lava[df] = filter_lava(df_lava[df])
    elif df == 'cats':
        df_lava[df] = filter_lava(df_lava[df])
    # else if df is not on lava_datasets list, print an error:
    else:
        print('ERROR: LAVA dataset "' + df + '" not recognized.')

#%% MERGE INPUT DATA WITH LAVA DATASETS #####################################
# Merge Input Data with LAVA Data
df_merged = merge_data(df_input, df_lava)

#%% EXPORT MERGED DATA TO EXCEL IN OUTPUT DIRECTORY #########################
# Output df_merge as xlsx file in specified output directory:
df_merged.to_excel(output_dir + 'merged_data.xlsx', index=False)

#%%
#############################################################################
#############################################################################
#############################################################################
#############################################################################
#############################################################################
#%%
# Import LAVA Data Dictionary:
lava_dict = import_lava_dict(lava_queries_dir)
#%% FINAL TRIM OF MERGED DATA ###############################################
# Create a copy of df_merged
df_merged_copy = df_merged.copy()
#%%
# Create a list of column names to keep:
cols_to_keep = ['PIDN',
'DCDate',
'DOB',
'DOD',
'bedside_DCDate',
'BNTCorr',
'Calc',
'CATSAMTot',
'CATSFMTot',
'cdr_DCDate',
'BoxScore',
'CDRTot',
'cvlt_DCDate',
'Corr10',
'Recog',
'DFCorr',
'DigitBW',
'DigitFW',
'language_DCDate',
'mmse_DCDate',
'MMSETot',
'moca_DCDate',
'MocaTotWithoutEduc',
'Rey10m',
'ModRey',
'MTCorr',
'MTError',
'MTTime',
'MSERep',
'PPGMSEDR',
'PPGMSEASR',
'ppt_DCDate',
'wrat4_DCDate',
'Pentgon',
'PPTP14Cor',
'PPTPCor',
'ReadIrr',
'ReadReg',
'Repeat5',
'Sex',
'StrpCNCor',
'StrpCor',
'Syntax',
'GDSTot',
'Verbal',
'ANCorr',
'DCorr',
'NumbLoc',
'AWRTot',
'SeqCmTot',
'CmpTot',
'RepTot',
'SSFluen',
'WRATTot']
#%%
#  Drop all columns from df_merged_copy that are not in cols_to_keep, but check if each column exists first:
for col in df_merged_copy.columns:
    if col not in cols_to_keep:
        if col in df_merged_copy.columns:
            df_merged_copy = df_merged_copy.drop(columns=col)

#%%

# # Rename columns in df_merged_copy to include descriptions from lava_dict by matching the column names to FieldName in lava_dict and the descriptions to FieldDescription in lava_dict
# df_merged_copy = df_merged_copy.rename(columns=lava_dict.set_index('FieldName')['FieldDescription'].to_dict())
# Rename columns in df_merged_copy to descriptions from lava_dict by matching column names to FieldName and pulling descriptions from FieldDescription. Do not rename any columns that do not have a match in lava_dict or that produce a NaN value.
df_merged_copy = df_merged_copy.rename(columns={col: lava_dict.loc[lava_dict['FieldName'] == col, 'FieldDescription'].values[0] for col in df_merged_copy.columns if col in lava_dict['FieldName'].values and not pd.isnull(lava_dict.loc[lava_dict['FieldName'] == col, 'FieldDescription'].values[0])})
# Export df_merged_copy to excel in output directory:
df_merged_copy.to_excel(output_dir + 'merged_data_descriptions.xlsx', index=False)

# %%
df_lava['other_cog']=import_lava_query(lava_queries_dir,'other_cog')
column_filter = ['Version','InstrType','VType','ResearchStatus','QualityIssue','QualityIssue2','QualityIssue3','InstrID','MemImp','ExecImp','LangImp','VisImp','BehImp','MotImp','PsyImp','OthImp','OtherDesc','ProjName','FundingProj','ProjPercent','FundingProj2','Proj2Percent','FundingProj3','Proj3Percent']
dcstatus_filter = ['Complete']
imagelinked_filter = ['NONE LINKED']
df = df_lava['other_cog']
# %%
for column in column_filter:
    if column in df.columns:
        df = df.drop(columns=column)
# Remove Timestamp from DCDate, leaving only YYYY-MM-DD format:
df['DCDate'] = pd.to_datetime(df.DCDate, errors='coerce').dt.date
# Drop all rows with NaT values in DCDate column:
df.dropna(subset=['DCDate'], inplace=True)
# Replace all negative values in dataset with NaN ignoring strings and datetime64 objects:
df = df.apply(lambda x: x.mask(x < 0, np.nan) if x.dtype.kind in 'biufc' else x)
# Create HELPERID values and insert as first column (PIDN_DCDate):
df.insert(loc=0 , column = 'HELPERID' , value = (df['PIDN'].map(str) + "_" + df['DCDate'].map(str)))
# Merge rows in df with the same HELPERID values and closest DCDate values within 30 days, prioritizing cells with data over cells with NaN values:
df = df.groupby('HELPERID').apply(lambda x: x.sort_values('DCDate', ascending=False).ffill(limit=1).bfill(limit=1))

# Show the value in column WRATTot for HELPERID: 10004_2009-11-13
df.loc[df['HELPERID'] == '10004_2009-11-13', 'WRATTot']


# export df to excel in output directory::
df.to_excel(output_dir + 'temp_other_cog_3.xlsx', index=False)


#%%



