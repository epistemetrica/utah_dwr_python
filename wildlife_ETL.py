# ETL Pipeline for wildlife lab results
# See https://github.com/epistemetrica/utah_dwr_python for more info

#import libraries
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine

#define handy functions
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

#create SQLlite db 
engine = create_engine('sqlite:///data/wildlife.db')


#define bison etl
def bison_etl():
    #read sample sheet; this will be the final table 
    bison_table = pd.read_excel('data/Bison_2021_22_Sample sheet.xlsx', usecols=[0, 1, 2, 3, 4, 5])
    #give pythonic columns names
    bison_table.columns = ['sample_id', 'archive_id', 'species', 'sex', 'capture_date', 'capture_unit']
    #strip strings
    bison_table = panda_stripper(bison_table)

    #read bvd type 1 excel table and strip strings
    bvd_type1_df = pd.read_excel('data/bison_tables.xlsx', sheet_name='bd_diarrhea_type1a', usecols=[0,2])
    bvd_type1_df = panda_stripper(bvd_type1_df)
    #get sample_id
    bvd_type1_df = get_sample_id(bvd_type1_df, 'Animals::Specimens')
    #drop 'Animals::Specimens' column
    del bvd_type1_df['Animals::Specimens']
    #reorder columns
    bvd_type1_df = bvd_type1_df.reindex(columns=['sample_id', 'Titer'])
    #rename columns
    bvd_type1_df.columns = ['sample_id', 'bvd_type1_result']
    #trim results
    bvd_type1_df.bvd_type1_result = bvd_type1_df.bvd_type1_result.apply(lambda row: row.split()[0])
    #merge into main table
    bison_table = bison_table.merge(bvd_type1_df, on= 'sample_id', how='outer')

    #load bvd type 2 tests and strip strings
    bvd_type2_df = pd.read_excel('data/bison_tables.xlsx', sheet_name='bv_diarrhea_type2', usecols= [0,2])
    bvd_type2_df = panda_stripper(bvd_type2_df)
    #get sample_id
    bvd_type2_df = get_sample_id(bvd_type2_df, 'Animals::Specimens')
    #drop 'Animals::Specimens' column
    del bvd_type2_df['Animals::Specimens']
    #reorder columns
    bvd_type2_df = bvd_type2_df.reindex(columns= ['sample_id', 'Titer'])
    #rename columns
    bvd_type2_df.columns = ['sample_id', 'bvd_type2_result']
    #trim results
    bvd_type2_df.bvd_type2_result = bvd_type2_df.bvd_type2_result.apply(lambda row: row.split()[0])
    #merge into main table
    bison_table = bison_table.merge(bvd_type2_df, on= 'sample_id', how='outer')

    #load EHDV data and strip
    ehdv_df = pd.read_excel('data/bison_tables.xlsx', sheet_name='ehdv', usecols=[0,2])
    ehdv_df = panda_stripper(ehdv_df)
    #drop extra rows
    ehdv_df = ehdv_df.drop(ehdv_df[ehdv_df['Animals::Specimens'] == ':: Serum'].index)
    #get sample_id
    ehdv_df = get_sample_id(ehdv_df, 'Animals::Specimens')
    #drop animals/specimens colums
    del ehdv_df['Animals::Specimens']
    #reorder columns
    ehdv_df = ehdv_df.reindex(columns= ['sample_id', 'Result'])
    #rename columns
    ehdv_df.columns = ['sample_id', 'ehdv_result']
    #merge into bison_table
    bison_table = bison_table.merge(ehdv_df, on= 'sample_id', how='outer')

    #load and strip bluetongue data
    bluepreg_df = pd.read_excel('data/bison_tables.xlsx', sheet_name='bluetongue')
    bluepreg_df = panda_stripper(bluepreg_df)
    #name columns
    bluepreg_df.columns = ['animal', 'preg_val', 'preg_result', 'bluetongue_result']
    #get sample_id
    bluepreg_df = get_sample_id(bluepreg_df, 'animal')
    #drop animal col
    del bluepreg_df['animal']
    #merge into bison_table
    bison_table = bison_table.merge(bluepreg_df, on= 'sample_id', how='outer')

    #export to .xlsx format for vet office
    bison_table.to_excel('data/finals/Bison 2021-2022 Lab Results.xlsx', index=False)

    #load df to db
    bison_table.to_sql('bison_table', engine, index=False, if_exists='replace')



def main():
    engine = create_engine('sqlite:///data/wildlife.db')
    bison_etl()
    


if __name__ == '__main__':
    main()