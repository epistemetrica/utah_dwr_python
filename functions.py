import re

def panda_stripper(df):
    '''Strips all string columns in a pandas dataframe, in place. Seems like this should already be a pd method, but whatever.'''
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda row: row.str.strip())
    return df

def get_sample_id(df, column):
    '''
    Extracts sample_id from a df column. 
    INPUT: df, column name
    Expected format: "BCB111 / BIS21-027 :: Serum"
    Action: splits string by whitespace or commas and returns first element of string
    OUTPUT: adds df column named 'sample_id' with the returned value in each cell
    '''
    df['sample_id'] = df[column].apply(lambda row: re.split(r"\s|,", row)[0])
    return df

