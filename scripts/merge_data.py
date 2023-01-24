# Merge Data Functions
# Import Packages
import pandas as pd
import numpy as np

# Merge LAVA data with input list based on PIDN and DCDate
def merge_data(df_input, df_lava, tolerance = '365 Days'):
    """
    Merge Data - Main Function
    """
    # Copy df_input as df_merged dataframe
    df_merged = df_input.copy()
    if 'DCDate' in df_merged.columns:
            # Convert DCDate column into Date Values leaving only YYYY-MM-DD format and sort values by DCDate:
            df_merged['DCDate'] = pd.to_datetime(df_merged['DCDate'])
            df_merged.sort_values('DCDate', inplace=True)
    # Set tolerance based on tolerance input
    tolerance = pd.Timedelta(tolerance)
    # 
    for df in df_lava:
        # Create a copy of the df_lava[df] dataframe input as df
        df_temp = df_lava[df].copy()
        # If DCDate exists in df, convert to datetime and sort by DCDate
        if 'DCDate' in df_temp.columns:
            # Convert DCDate column into Date Values leaving only YYYY-MM-DD format and sort values by DCDate:
            df_temp['DCDate'] = pd.to_datetime(df_temp['DCDate'])
            df_temp.sort_values('DCDate', inplace=True)
            # Append df to beginning of every column name in df_lava[df] EXCEPT the 'PIDN' column and 'DCDate' column:
            ###df_lava[df].columns = ['PIDN', 'DCDate'] + [df + '_' + str(col) for col in df_lava[df].columns if col not in ['PIDN', 'DCDate']]
            # Create a copy of the DCDate column to the right of the PIDN column and append the df name to the column name:
            df_temp.insert(1, df + '_DCDate', df_temp['DCDate'])
            # Merge df_lava[df] to df_merged based on PIDN and DCDate:
            df_merged = pd.merge_asof(left=df_merged, right=df_temp, on='DCDate', by='PIDN', direction='nearest', tolerance=tolerance)
        elif 'DCDate' not in df_lava[df].columns:
            # Merge df_lava[df] to df_merged based on PIDN only:
            df_merged = pd.merge(df_merged, df_temp, on='PIDN', how='left')
    return df_merged

# Check for duplicate PIDNs in dataframe
def duplicate_pidn_check(df):
    """
    Check for duplicate PIDNs in dataframe.
    """
    # Check if there are duplicate pidns in cases with no dates
    if df['PIDN'].nunique() != len(df['PIDN']):
        raise ValueError('Duplicate PIDN values exist in dataframe. Cannot run without specifying a date column.')
    else:
        print('No duplicate PIDNs found in dataframe.')
    return