import pandas as pd
import os
import sys
import numpy as np
from decimal import Decimal
from pathlib import Path

# cwd =  Path.cwd()

# folder = cwd /'IB Activity'

# tables = cwd / 'Tables'


def process_ca(data):
    data_ca = data.loc[data.a =='Corporate Actions']
    if len(data_ca) > 0:
        ca = data_ca.loc[data_ca.b == 'Data']
        if len(ca) > 0:
            first_row = ca.index.values[0]-1
            header_row = list(data_ca.loc[first_row])
            ca.columns = header_row
            ca = ca.loc[:,ca.columns.notna()] 
            ca.drop(columns=['Corporate Actions','Header','Report Date','Description','Value','Realized P/L','Code'], inplace=True, errors='ignore')
            ca.dropna(axis=0,inplace=True)
            ca['Comm/Fee'] = 0
            ca['T. Price'] = 0
            return ca


def convert_to_df(a_file):
    #file_path = os.path.join('Ib Activity', a_file)
    #data = pd.read_csv(file_path, names=list('abcdefghijklmnopq'))
    data = pd.read_csv(a_file, names=list('abcdefghijklmnopq'))
    #data.drop(data.loc[(data.a == 'Notes/Legal Notes') | (data.a == 'Codes') ].index, inplace= True)
    #data = (data.a != 'Codes') & (data.a != 'Notes/Legal Notes')
    data = data.loc[~data.a.isin(['Statement','Codes', 'Notes/Legal Notes','Account Information','Change in NAV','Mark-to-Market Performance Summary','Realized & Unrealized Performance Summary','Open Positions'])]
    return data

def process_data(data, asset_category):
    data_trades = data.loc[data.a =='Trades']
    if len(data_trades) > 0:
        trades = data_trades.loc[data_trades.d == asset_category]
        if len(trades) > 0:
            first_row = trades.index.values[0]-1
            header_row = list(data_trades.loc[first_row])
            trades.columns = header_row
            trades = trades.loc[trades.Header =='Data']
            trades = trades.loc[:,trades.columns.notna()]
            trades.drop(columns=['Trades','Header','DataDiscriminator','C. Price','Basis','Realized P/L','MTM P/L','Code', 'MTM in USD', 'Notional Value'], inplace=True, errors='ignore')
            trades.rename(columns={'Comm in USD':'Comm/Fee'},inplace=True, errors='ignore')
            if 'Proceeds' not in trades.columns: trades['Proceeds'] = 0
            trades['Quantity'] = trades['Quantity'].str.replace(',', '')
            return trades

def get_all_trades(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    dataframe_list = []
    #Iterates through all files in filelist
    asset_categories_lst = ['Stocks','Equity and Index Options','Futures','Options On Futures', 'Forex']
    for a_file in filelist:
        #Checks if file is a .csv
        if a_file.lower().endswith('.csv'):
            data = convert_to_df(a_file)
            for i in asset_categories_lst:
                df = process_data(data, i)
                dataframe_list.append(df)
            dataframe_list.append(process_ca(data))
    new_trades = pd.concat(dataframe_list,sort=False )
    dataframe = pd.concat(dataframe_list,sort=False )
    dataframe['Proceeds'] = dataframe['Proceeds'].astype('float')
    dataframe['Quantity'] = dataframe['Quantity'].astype('float')
    dataframe['T. Price'] = dataframe['T. Price'].astype('float')
    dataframe['Comm/Fee'] = dataframe['Comm/Fee'].astype('float')
    dataframe['Date/Time'] = pd.to_datetime(dataframe['Date/Time'])
    dataframe['Quantity_Rsum'] =  dataframe.groupby(['Symbol'])['Quantity'].transform(lambda x : x.cumsum())
    return dataframe

def calculate_PL (df):
    newdf = df.loc[:,['Quantity','Quantity_Rsum','T. Price','Proceeds']]
    newdf['prev'] = newdf['Quantity_Rsum'].shift(1 , fill_value=0)
    #Make a new column OIDC
    oidc_options = [
        newdf['prev'] == 0,
        newdf['Quantity_Rsum'] == 0,
        newdf['prev']/newdf['Quantity_Rsum'] < 0,
        abs(newdf['Quantity_Rsum']) > abs(newdf['prev'])
    ]
    oidc_choices = [
        'Open','Close','Reversal', 'Increase'
    ]
    newdf['oidc'] = np.select(oidc_options, oidc_choices, default = 'Decrease')

    newdf['index'] = np.where((newdf['oidc']== 'Open')|(newdf['oidc']=='Reversal'),1,0)

    newdf['Increase'] = np.where((newdf['oidc'] == 'Open') | (newdf['oidc'] == 'Increase') | (newdf['oidc'] =='Reversal'),1,0)
    newdf['ProceedsIncrease'] = newdf['Proceeds']*newdf['Increase']
    newdf['QuantityIncrease'] = newdf['Quantity']*newdf['Increase']
    newdf['cumsum'] = newdf['index'].cumsum()
    newdf['TotProceedsIncrease']= newdf.groupby(['cumsum'])['ProceedsIncrease'].cumsum()
    newdf['TotQuantityIncrease']= newdf.groupby(['cumsum'])['QuantityIncrease'].cumsum()
    newdf['AvgOpenPrice'] = newdf['TotProceedsIncrease'] / newdf['TotQuantityIncrease'] * -1
    newdf['AvgOpenPriceBefore'] = newdf['AvgOpenPrice'].shift(1 , fill_value=0)

    plOptions = [
        newdf['oidc'] == 'Open',
        newdf['oidc'] == 'Increase',
        newdf['oidc'] == 'Decrease',
        newdf['oidc'] == 'Close',
        newdf['oidc'] == 'Reversal'
    ]
    plChoices = [
        # Open case P/L = 0
        0,
        #Increase case 
        0,
        #Decrease case: Average Open Price - T. Price * Quantity (* Multiplier +coommfee)
        (newdf['AvgOpenPriceBefore'] - newdf['T. Price']) * newdf['Quantity'],
        #Close case 
        (newdf['AvgOpenPriceBefore'] - newdf['T. Price']) * newdf['Quantity'],
        #Reverse case
        (newdf['AvgOpenPriceBefore'] - newdf['T. Price']) * newdf['Quantity']
    ]
    newdf['PL'] = np.select(plOptions, plChoices, default = 0)
    newdf['CumPL'] = newdf['PL'].cumsum()
    newdf.drop(['Quantity',	'Quantity_Rsum', 'T. Price', 'Proceeds', 'prev', 'oidc', 'index','Increase','ProceedsIncrease', 'QuantityIncrease', 'cumsum', 'TotProceedsIncrease','TotQuantityIncrease','AvgOpenPriceBefore'], axis=1, inplace=True)
    #return newdf['PL'], newdf['AvgOpenPrice'], newdf['CumPL']
    return newdf

def updatePL (df):
    df.drop(['AvgOpenPrice','PL','CumPL'], axis=1, inplace = True, errors= 'ignore')
    dfgrouped = df.groupby('Symbol')
    dataframe_list = []
    for name, group in dfgrouped:
        symbol_group = dfgrouped.get_group(name)
        profLoss = calculate_PL( symbol_group)
        symbol_group = symbol_group.join(profLoss)
        dataframe_list.append(symbol_group)
    new_trades = pd.concat(dataframe_list,sort=False ).sort_index()
    return new_trades

# ##if 'tables_Trades.csv' in cwd/'Tables':
# trades = pd.read_csv(cwd/'Tables'/'tables_Trades.csv')
# newTrades = get_all_trades(folder)
# trades = trades.append(newTrades)
# # else:
# #     trades =  get_all_trades(folder)
# trades.to_csv(cwd/'Tables'/'tables_Trades.csv')




