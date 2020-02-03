import pandas as pd
import os
import sys
import numpy as np


files = os.listdir(folder)

def convert_to_df(file):
    file_path = os.path.join('Ib Activity', file)
    data = pd.read_csv(file_path, names=list('abcdefghijklmnopq'))
    data = data.loc[~data.a.isin(['Codes', 'Notes/Legal Notes','Account Information','Change in NAV','Mark-to-Market Performance Summary','Realized & Unrealized Performance Summary','Open Positions'])]
    return data

def process_depAndWith(data):
    data_depAndWith = data.loc[data.a == 'Deposits & Withdrawals']
    if len(data_depAndWith) > 0:
        depAndWith = data_depAndWith.loc[data_depAndWith.b == 'Data']
        first_row = depAndWith.index.values[0]-1
        header_row = list(data_depAndWith.loc[first_row])
        depAndWith.columns = header_row
        depAndWith = depAndWith.dropna(subset=['Settle Date'])
        depAndWith = depAndWith.loc[:,depAndWith.columns.notna()]
        depAndWith.drop(columns=['Header','Deposits & Withdrawals'], inplace=True, errors='ignore')
        depAndWith['Date'] = pd.to_datetime(depAndWith['Settle Date'])
        depAndWith = depAndWith.groupby(['Date','Currency'], as_index=False).sum()
    else:
        depAndWith = pd.DataFrame()
    return depAndWith

def get_all_depAndWith(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    depAndWith_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(file_)
            depAndWith = process_depAndWith(data)
            #if type(div) != 'NonType':
            if len(depAndWith) !=0:
                depAndWith_list.append(depAndWith)
    deposits_and_withdrawals = pd.concat(depAndWith_list,sort=False )
    return deposits_and_withdrawals