# Filter Data Functions
# Import Packages
import pandas as pd
import numpy as np

column_filter = ['Version','InstrType','VType','ResearchStatus','QualityIssue','QualityIssue2','QualityIssue3','InstrID','MemImp','ExecImp','LangImp','VisImp','BehImp','MotImp','PsyImp','OthImp','OtherDesc','ProjName','FundingProj','ProjPercent','FundingProj2','Proj2Percent','FundingProj3','Proj3Percent']
dcstatus_filter = ['Complete']
imagelinked_filter = ['NONE LINKED']

# Filter NI_ALL:
def filter_ni_all(df,
                 scannerid_filter = ['SFVA 1.5T MRI','NIC 3T MRI','NIC 3T MRI PRISMA','SFVA 4T MRI']):
    """
    Filter Raw LAVA_NI_ALL Data
    """
    # Main Filters:
    # Columns we like: ['PIDN','DCDate','DCStatus','AgeAtDC','ScannerID','SourceID','ScanType','ImageLinked','ImgPath','ImgFormat','ScannerNotes','ImgQuality','QualityNotes']

    # Filter out any columns not in column_filter list:
    for column in column_filter:
        if column in df.columns:
            df = df.drop(columns=column)
    # Filter out any cases without "Complete" visit DCStatus and drop DCStatus column::
    df = df[df['DCStatus'].isin(dcstatus_filter)]
    df = df.drop(columns=['DCStatus'], axis=1)
    # Filter out any cases that aren't in ScannerID filter list:
    df = df[df['ScannerID'].isin(scannerid_filter)]
    # Filter out any cases that are listed as "NONE LINKED" in ImageLinked list:
    df = df[-df['ImageLinked'].isin(imagelinked_filter)]
    # Remove Timestamps from DCDate, leaving only YYYY-MM-DD format and convert NaN to NaT:
    df['DCDate'] = pd.to_datetime(df.DCDate, errors='coerce').dt.date
    # Drop all rows with NaT values in DCDate column:
    df.dropna(subset=['DCDate'], inplace=True)
    # Create HELPERID values and insert as first column (PIDN_SourceID_DCDate):
    df.insert(loc=0 , column = 'HELPERID' , value = (df['PIDN'].map(str) + "_" + df['SourceID'].map(str) + "_" + df['DCDate'].map(str)))
    df
    return (df)

# Filter, Sort, and Trim for T1 Scans:
def filter_t1(df, t1_filter = ['T1-long','T1-long-3DC','T1-short','T1-short-3DC']):
    """
    Filter Neuroimaging Data to T1 Scans only
    Sort by PIDN (Smallest->Biggest),
    then by DCDate (Oldest->Newest), 
    then by ScanType (A->Z), priotizing: T1-long > T1-long-3DC > T1-short > T1-short-3DC
    then by ImgFormat (Z->A), prioritizing: NifTI > DICOM > Analyze
    Trim duplicates by HELPERID
    Reset index
    """
    # Filter out any cases that aren't in T1 ScanType list:
    df = df[df['ScanType'].isin(t1_filter)]
    # Sort by: PIDN (Smallest->Biggest), then by DCDate (Oldest->Newest), then by ScanType (A->Z), priotizing: T1-long > T1-long-3DC > T1-short > T1-short-3DC, then by ImgFormat (Z->A), prioritizing: NifTI > DICOM > Analyze
    df = df.sort_values(by = ['PIDN','DCDate','ScanType','ImgFormat'], ascending=[True,True,True,False])
    # Trim duplicates by HELPERID
    df = df.drop_duplicates(subset=['HELPERID'],keep='first')
    # Reset index
    df.reset_index(drop=True)
    return (df)

# Filter Demographics:
def filter_demographics(df):
    """
    Filter Raw LAVA_Demographics Data
    """
    # Main Filters:
    # COLUMNS WE LIKE: ['PIDN','DOB','Deceased','DOD','Hand','Gender','Educ','Onset','VerifiedOnset']
    # If any columns exist in df in column_filter list, drop them:
    for column in column_filter:
        if column in df.columns:
            df = df.drop(columns=column)
    # # Remove Timestamp from DOB, leaving only YYYY-MM-DD format and convert NaN to NaT:
    df['DOB'] = pd.to_datetime(df.DOB, errors='coerce').dt.date
    # Remove Timestamp from DOD, leaving only YYYY-MM-DD format and convert NaN to NaT:
    df['DOD'] = pd.to_datetime(df.DOD, errors='coerce').dt.date
    return (df)

# Filter LAVA Diagnosis Data:
def filter_diagnosis(df):
    """
    Filter Raw LAVA Diagnosis Data
    """
    # If any columns exist in df in column_filter list, drop them:
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
    return (df)

# Filter General LAVA Data:
def filter_lava(df):
    """
    Filter Raw LAVA Data
    This works on most LAVA datasets, but may need to be modified for specific LAVA datasets.
    """
    # If any columns exist in df in column_filter list, drop them:
    for column in column_filter:
        if column in df.columns:
            df = df.drop(columns=column)
    # Remove any rows that aren't in dcstatus_filter list if DCStatus column exists:
    if 'DCStatus' in df.columns:
        df = df[df['DCStatus'].isin(dcstatus_filter)]
    # Remove Timestamp from DCDate, leaving only YYYY-MM-DD format:
    df['DCDate'] = pd.to_datetime(df.DCDate, errors='coerce').dt.date
    # Drop all rows with NaT values in DCDate column:
    df.dropna(subset=['DCDate'], inplace=True)
    # Replace all negative values in dataset with NaN ignoring strings and datetime64 objects:
    df = df.apply(lambda x: x.mask(x < 0, np.nan) if x.dtype.kind in 'biufc' else x)
    # Create HELPERID values and insert as first column (PIDN_DCDate):
    df.insert(loc=0 , column = 'HELPERID' , value = (df['PIDN'].map(str) + "_" + df['DCDate'].map(str)))
    return (df)

# Filter Other_Cog LAVA Data:
def filter_other_cog(df):
    """
    Filter Raw LAVA OtherCog Data
    """
    # If any columns exist in df in column_filter list, drop them:
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
    # Merge data that has empty values with the values from other rows if they have the same HELPERID:
    #df = df.groupby('HELPERID').ffill().bfill()
    return (df)

# Filter Timepoints:
def filter_timepoints(df, timepoint='first'):
    """
    Filter DCDate Timepoints based on timepoint argument:

    Timepoint options:
    'first' = Earliest valid DCDate for each PIDN
    'latest' = Latest valid DCDate for each PIDN
    'fullest' = Fullest dataset for each PIDN
    """
    # Sort by DCDate (Oldest->Newest):
    df = df.sort_values(by = ['PIDN','DCDate'], ascending=[True,True])
    print('Sorting Cases by DCDate')
    # If timepoint is "first", filter for earliest DCDate for each PIDN:
    if timepoint == 'first':
        df = df.groupby('PIDN').first().reset_index()
        print('First Timepoint Filtered')
    # If timepoint is "latest", filter df for latest DCDate for each PIDN:
    elif timepoint == 'latest':
        df = df.groupby('PIDN').last().reset_index()
        print('Latest Timepoint Filtered')
    # If timepoint is "fullest", filter df for max count for each PIDN:
    elif timepoint == 'fullest':
        # Count how many values there are per row:
        df = df
        df['count'] = df.count(axis=1)
        # Sort by max count:
        df = df.sort_values(by = ['PIDN','count'], ascending=[True,False])
        # Filter for max count:
        df = df.groupby('PIDN').first().reset_index()
        # Remove count column:
        df = df.drop(['count'], axis=1)
        print('Fullest Timepoint Filtered')
    else:
        print('Invalid Timepoint Selection')
    return (df)
