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


#etl deer
def deer_etl():
    #read excel table and strip
    deer_table = pd.read_excel('data/Muledeer_2021_22_Sample sheet.xlsx', usecols=[0,1,2,3,4,5])
    deer_table = panda_stripper(deer_table)
    #rename columns
    deer_table.columns = ['sample_id', 'collar_id', 'species', 'sex', 'capture_date', 'capture_unit']

    #read and strip adenovirus table
    adenovirus_df = pd.read_excel('data/deer_tables.xlsx', sheet_name='adenovirus')
    adenovirus_df = panda_stripper(adenovirus_df)
    #get sample id
    adenovirus_df = get_sample_id(adenovirus_df, 'sample_id')
    #combine result1 and result2 cols
    adenovirus_df['adenovirus_result'] = adenovirus_df.apply(lambda row: row['result1'] if row['result1'] is not np.NaN else row['result2'], axis=1)
    del adenovirus_df['result1']
    del adenovirus_df['result2']
    #trim "Negative @" result
    adenovirus_df['adenovirus_result'] = adenovirus_df['adenovirus_result'].apply(lambda row: row.split()[0])
    #merge with deer_table
    deer_table = deer_table.merge(adenovirus_df, on='sample_id', how='outer')

    #read and strip EHDV table
    ehdv_df = pd.read_excel('data/deer_tables.xlsx', sheet_name='ehdv', usecols=[0,2])
    ehdv_df = panda_stripper(ehdv_df)
    #get sample_id and drop original col
    ehdv_df = get_sample_id(ehdv_df, 'Animals::Specimens')
    del ehdv_df['Animals::Specimens']
    #rename cols
    ehdv_df.columns = ['ehdv_result', 'sample_id']
    #merge with deer_table
    deer_table = deer_table.merge(ehdv_df, on='sample_id', how='outer')

    #read and strip bluetonge table
    bluetongue_df = pd.read_excel('data/deer_tables.xlsx', sheet_name='bluetongue')
    bluetongue_df = panda_stripper(bluetongue_df)
    #get sample_id and drop original col
    bluetongue_df = get_sample_id(bluetongue_df, 'specimen')
    del bluetongue_df['specimen']
    #rename cols
    bluetongue_df.columns = ['bluetongue_result', 'sample_id']
    #merge with deer_table
    deer_table = deer_table.merge(bluetongue_df, on='sample_id', how='outer')

    #export to xlsx for office use
    deer_table.to_excel('data/finals/Mule Deer 2021-2022 Lab Results.xlsx', index=False)

    #load to db
    deer_table.to_sql('deer_table', engine, index=False, if_exists='replace')


#define elk etl 
def elk_etl():
    #read main table and strip
    elk_table = pd.read_excel('data/Elk captures 21-22.xlsx')
    elk_table = panda_stripper(elk_table)
    #rename cols
    elk_table.columns = ['sample_id', 'archive_id', 'collar_id', 'species', 'sex', 'capture_date', 'capture_unit', 'staging_area', 'age', 'comments']

    #read and strip ehdv table
    ehdv_raw_df = pd.read_excel('data/elk_tables.xlsx', sheet_name='ehdv', usecols=[0,1])
    ehdv_raw_df = panda_stripper(ehdv_raw_df)
    #select every 3rd row starting with the 1st row
    ehdv_df = ehdv_raw_df.iloc[0::3]
    #reset index
    ehdv_df = ehdv_df.reset_index(drop=True)
    #rename cols
    ehdv_df.columns = ['test', 'animal']
    #extract result and titer from ehdv_raw_df (i.e., every 3rd row starting with 2nd row)
    df = ehdv_raw_df.iloc[2::3]
    del df['animal']
    #reset index
    df = df.reset_index(drop=True)
    df.columns = ['result']
    #merge
    ehdv_df = pd.concat([ehdv_df, df], axis=1)
    #get sample ids
    ehdv_df['sample_id'] = ehdv_df['animal'].apply(lambda row: re.split(r"\s|/", row)[2][:7])
    del ehdv_df['animal']
    #split into results and values
    ehdv_df['val'] = ehdv_df.result.apply(lambda row: row.split()[1])
    ehdv_df['result'] = ehdv_df.result.apply(lambda row: row.split()[0])
    #split ehdv tests
    ehdv1_df = ehdv_df[ehdv_df.test == 'Test: Epizootic Hemorrhagic Disease Virus Type 1 (VN)']
    ehdv2_df = ehdv_df[ehdv_df.test == 'Test: Epizootic Hemorrhagic Disease Virus Type 2 (VN)']
    ehdv6_df = ehdv_df[ehdv_df.test == 'Test: Epizootic Hemorrhagic Disease Virus Type 6 (VN)']
    #rename cols
    ehdv1_df.columns = ['test', 'ehdv_type1_result', 'sample_id', 'ehdv_type1_val']
    ehdv2_df.columns = ['test', 'ehdv_type2_result', 'sample_id', 'ehdv_type2_val']
    ehdv6_df.columns = ['test', 'ehdv_type6_result', 'sample_id', 'ehdv_type6_val']
    #drop unneeded cols
    del ehdv1_df['test']
    del ehdv2_df['test']
    del ehdv6_df['test']
    #merge the 3 dfs into single tidy ehdv df
    ehdv_df = ehdv1_df.merge(ehdv2_df, on='sample_id', how='outer')
    ehdv_df = ehdv_df.merge(ehdv6_df, on='sample_id', how='outer')
    #merge into main table
    elk_table = elk_table.merge(ehdv_df, on='sample_id', how='outer')

    #read and trip BVD table
    bvd_df = pd.read_excel('data/elk_tables.xlsx', sheet_name='bv_diarrhea')
    bvd_df = panda_stripper(bvd_df)
    #get sample ids
    bvd_df['sample_id'] = bvd_df['label'].apply(lambda row: re.split(r"\s|/", row)[1])
    #drop label col
    del bvd_df['label']
    #rename cols
    bvd_df.columns = ['bvd_result', 'sample_id']
    #merge with main table
    elk_table = elk_table.merge(bvd_df, on='sample_id', how='outer')
    #read and trip table
    bvd_df = pd.read_excel('data/elk_tables.xlsx', sheet_name='bv_diarrhea')
    bvd_df = panda_stripper(bvd_df)
    #get sample ids
    bvd_df['sample_id'] = bvd_df['label'].apply(lambda row: re.split(r"\s|/", row)[1])
    #drop label col
    del bvd_df['label']
    #rename cols
    bvd_df.columns = ['bvd_result', 'sample_id']
    #merge with main table
    elk_table = elk_table.merge(bvd_df, on='sample_id', how='outer')

    #read and strip pregnancy results table
    preggers_df = pd.read_excel('data/elk_tables.xlsx', sheet_name='preg', usecols=[1,2,3])
    preggers_df = panda_stripper(preggers_df)
    #get sample ids
    preggers_df['sample_id'] = preggers_df['animal_id'].apply(lambda row: row[:7])
    #drop unneeded col
    del preggers_df['animal_id']
    #rename cols
    preggers_df.columns = ['preg_OD_val', 'preg_result', 'sample_id']
    #merge into main table
    elk_table = elk_table.merge(preggers_df, on='sample_id', how='outer')

    #read and strip bluetongue table
    bt_df = pd.read_excel('data/elk_tables.xlsx', sheet_name='bluetongue')
    bt_df = panda_stripper(bt_df)
    #get sample id
    bt_df['sample_id'] = bt_df.label.apply(lambda row: row[:7])
    #drop unneeded col
    del bt_df['label']
    #rename
    bt_df.columns = ['bluetongue_result', 'sample_id']
    #merge with main table
    elk_table = elk_table.merge(bt_df, on='sample_id', how='outer')

    #export to excel for office use:
    elk_table.to_excel('data/finals/Elk 2021-2022 Lab Results.xlsx', index=False)

    #load to db
    elk_table.to_sql('elk_table', engine, index=False, if_exists='replace')


def main():
    engine = create_engine('sqlite:///data/wildlife.db')
    bison_etl()
    deer_etl()


if __name__ == '__main__':
    main()