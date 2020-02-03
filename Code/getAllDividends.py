import pandas as pd
import os
import sys
import numpy as np

folder = 'D:\Javier\SynologyDrive\Portfolio Python\IB Activity'

tables = 'D:\Javier\SynologyDrive\Portfolio Python\Tables'

files = os.listdir(folder)

def convert_to_df(file):
    file_path = os.path.join('Ib Activity', file)
    data = pd.read_csv(file_path, names=list('abcdefghijklmnopq'))
    data = data.loc[~data.a.isin(['Codes', 'Notes/Legal Notes','Account Information','Change in NAV','Mark-to-Market Performance Summary','Realized & Unrealized Performance Summary','Open Positions'])]
    return data

def process_div(data):
    data_div = data.loc[data.a == 'Dividends']
    if len(data_div) > 0:
        div = data_div.loc[data_div.b == 'Data']
        first_row = div.index.values[0]-1
        header_row = list(data_div.loc[first_row])
        div.columns = header_row
        div = div.dropna(subset=['Date'])
        div = div.loc[:,div.columns.notna()]
        div['Symbol'] = div['Description'].str.split('(', n = 1).str[0].str.strip()
        div['Gross Amount'] = div.Amount.astype(float)
        div.drop(columns=['Dividends','Data','Header','Tax','Description','Amount'], inplace=True, errors='ignore')
        div['Date'] = pd.to_datetime(div['Date'])
        div = div.groupby(['Date','Currency','Symbol'], as_index=False).sum()
    else:
        div = pd.DataFrame()
    return div

def process_tax(data):
    data_tax = data.loc[data.a == 'Withholding Tax']
    if len(data_tax) > 0:
        tax = data_tax.loc[data_tax.b == 'Data']
        first_row = tax.index.values[0]-1
        header_row = list(data_tax.loc[first_row])
        tax.columns = header_row
        tax = tax.dropna(subset=['Date'])
        tax = tax.loc[:,tax.columns.notna()]
        tax['Symbol'] = tax['Description'].str.split('(', n = 1).str[0].str.strip()
        tax['Tax Amount'] = tax.Amount.astype(float)
        tax.drop(columns=[ 'Withholding Tax','Data','Header','Tax','Code','Description','Amount'], inplace=True, errors='ignore')
        tax['Date'] = pd.to_datetime(tax['Date'])
        tax = tax.groupby(['Date','Currency','Symbol'], as_index=False).sum()
    else:
        tax = pd.DataFrame()    
    return tax

def get_all_dividends(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    div_list = []
    tax_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(file_)
            div = process_div(data)
            tax = process_tax(data)
            #if type(div) != 'NonType':
            if len(div) !=0:
                div_list.append(div)
            if len(tax) !=0:
                tax_list.append(tax)
    gross_dividends = pd.concat(div_list,sort=False )
    gross_dividends.groupby(['Date','Currency','Symbol'],as_index=False).sum()
    taxes = pd.concat(tax_list,sort=False )
    taxes.groupby(['Date','Currency','Symbol'],as_index=False).sum()
    dividends = pd.concat([gross_dividends,taxes], sort=False).groupby(['Date','Symbol','Currency']).sum().reset_index()
    dividends['Proceeds'] = dividends['Gross Amount'] + dividends['Tax Amount']
    return dividends