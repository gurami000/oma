import os
import pandas as pd
import numpy as np

def convert_to_df(folder,a_file):
    data = pd.read_csv(folder/a_file, names=list('abcdefghijklmnopq'))
    data = data.loc[~data.a.isin(['Codes', 'Notes/Legal Notes','Account Information','Change in NAV','Mark-to-Market Performance Summary','Realized & Unrealized Performance Summary','Open Positions'])]
    return data

def process_ca(data):
    data_ca = data.loc[data.a =='Corporate Actions']
    if len(data_ca) > 0:
        ca = data_ca.loc[data_ca.b == 'Data']
        if len(ca) > 0:
            first_row = ca.index.values[0]-1
            header_row = list(data_ca.loc[first_row])
            ca.columns = header_row
            ca = ca.loc[:,ca.columns.notna()] 
            ca.drop(columns=['Corporate Actions','Header','Report Date','Value','Realized P/L','Code'], inplace=True, errors='ignore')
            ca.dropna(axis=0,inplace=True)
            ca['Comm/Fee'] = 0
            ca['T. Price'] = 0
        else:
            ca = pd.DataFrame()
    else:
        ca = ca = pd.DataFrame()
    return ca

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
        else:
            trades = pd.DataFrame()
    else:
        trades = pd.DataFrame()
    return trades

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

def process_fees(data):
    data_ca = data.loc[data.a =='Fees']
    if len(data_ca) > 0:
        ca = data_ca.loc[data_ca.b == 'Data']
        if len(ca) > 0:
            first_row = ca.index.values[0]-1
            header_row = list(data_ca.loc[first_row])
            ca.columns = header_row
            ca = ca.loc[:,ca.columns.notna()] 
            ca.drop(columns=['Fees','Header'], inplace=True, errors='ignore')
            ca.dropna(axis=0,inplace=True)
        else:
            ca = pd.DataFrame()
    else:
        ca = pd.DataFrame()
    return ca

def process_pv(data):
    date = data.loc[data.a =='Statement']
    date = date.loc[date.c =='Period']
    date.dropna(axis=1,inplace=True)
    date.drop(columns=['a','b','c'], inplace=True, errors='ignore')
    date.reset_index(inplace=True,drop=True)
    date = date.iloc[0:1]
    date.columns = ['Date']
    data_pv = data.loc[data.a =='Net Asset Value']
    if len(data_pv) > 0:
        pv = data_pv.loc[data_pv.b == 'Data']
        if len(pv) > 0:
            first_row = pv.index.values[0]-1
            header_row = list(data_pv.loc[first_row])
            pv.columns = header_row
            pv = pv.loc[:,pv.columns.notna()]
            pv = pv.loc[pv['Asset Class'] == 'Total']
            pv.drop(columns=['Asset Class','Net Asset Value','Header','Prior Total', 'Current Long', 'Current Short','Change'], inplace=True, errors='ignore')
            pv.reset_index(inplace=True,drop=True)
            pv = date.join(pv)
            pv['Date'] = pd.to_datetime(pv['Date'])
            pv['Current Total'] =  pv['Current Total'].astype('float')
            #pv.set_index('Date',inplace=True)
            df = pd.DataFrame(data=pv, index=date)
        else:
            pv = pd.DataFrame()
    else:
        pv= pd.DataFrame()

    return pv

def process_DW_In_Base(data):
    date = data.loc[data.a =='Statement']
    date = date.loc[date.c =='Period']
    date.dropna(axis=1,inplace=True)
    date.drop(columns=['a','b','c'], inplace=True, errors='ignore')
    date.reset_index(inplace=True,drop=True)
    date = date.iloc[0:1]
    date.columns = ['Date']
    date['Date'] = pd.to_datetime(date['Date'])

    data_check = data.loc[data.a == 'Deposits & Withdrawals']
    if len(data_check) > 0:
        depAndWith = data.loc[data.a == 'Cash Report']
        depAndWith = depAndWith.loc[depAndWith.d == 'Base Currency Summary']
        depAndWith = depAndWith.loc[(depAndWith.c == 'Deposits')| (depAndWith.c == 'Withdrawals')]
        depAndWith.e = depAndWith.e.astype(float)
        # # depAndWith.set_index('b', inplace =True)
        depAndWith = depAndWith.groupby('a').sum()
        
      
        
        depAndWith = depAndWith.reset_index(drop=True)
        dw = pd.Series(depAndWith.e)
        date['DW_In_Base'] = dw
    else:
        date = pd.DataFrame()
    return date    


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
            data = convert_to_df(folder,a_file)
            for i in asset_categories_lst:
                df = process_data(data, i)
                dataframe_list.append(df)
            dataframe_list.append(process_ca(data))
            
    # if len(dataframe_list) == 0:
    #     dataframe = pd.DataFrame()
    else:
        #new_trades = pd.concat(dataframe_list,sort=False )
        dataframe = pd.concat(dataframe_list,sort=False )
        if len(dataframe) > 0:
            # dataframe['Proceeds'] = dataframe['Proceeds'].astype('float')
            # dataframe['Quantity'] = dataframe['Quantity'].astype('float')
            # dataframe['T. Price'] = dataframe['T. Price'].astype('float')
            # dataframe['Comm/Fee'] = dataframe['Comm/Fee'].astype('float')
            dataframe['Date/Time'] = pd.to_datetime(dataframe['Date/Time'])
            dataframe.reset_index(inplace=True, drop=True)
            dataframe.set_index('Date/Time').sort_index().reset_index(inplace=True)
            #dataframe['Quantity_Rsum'] =  dataframe.groupby(['Symbol'])['Quantity'].transform(lambda x : x.cumsum())
    return dataframe

def get_all_corporate_actions(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    ca_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(folder,file_)
            ca = process_ca(data)
            if len(ca) !=0:
                ca_list.append(ca)
    if len(ca_list) == 0:
        corporate_actions = pd.DataFrame()
    else:
        corporate_actions = pd.concat(ca_list,sort=False )
    return corporate_actions

def get_all_depAndWith(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    depAndWith_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(folder,file_)
            depAndWith = process_depAndWith(data)
            #if type(div) != 'NonType':
            if len(depAndWith) !=0:
                depAndWith_list.append(depAndWith)
    if len(depAndWith_list) == 0:
        deposits_and_withdrawals = pd.DataFrame()
    else:
        deposits_and_withdrawals = pd.concat(depAndWith_list,sort=False )
    return deposits_and_withdrawals

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
            data = convert_to_df(folder,file_)
            div = process_div(data)
            tax = process_tax(data)
            #if type(div) != 'NonType':
            if len(div) !=0:
                div_list.append(div)
            if len(tax) !=0:
                tax_list.append(tax)
    if len(div_list) == 0:
        dividends = pd.DataFrame()
    else:
        gross_dividends = pd.concat(div_list,sort=False )
        gross_dividends.groupby(['Date','Currency','Symbol'],as_index=False).sum()
        if len(tax_list) ==0:
            taxes = pd.DataFrame()
        else:
            taxes = pd.concat(tax_list,sort=False )
            taxes.groupby(['Date','Currency','Symbol'],as_index=False).sum()
        dividends = pd.concat([gross_dividends,taxes], sort=False).groupby(['Date','Symbol','Currency']).sum().reset_index()
        dividends['Proceeds'] = dividends['Gross Amount'] + dividends['Tax Amount']
    return dividends

def get_all_fees(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    fees_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(folder,file_)
            fee = process_fees(data)
            if type(fee) != 'NonType':
                fees_list.append(fee)
    if len(fees_list) == 0:
        fees = pd.DataFrame()
    else:
        fees = pd.concat(fees_list,sort=False )
    return fees

def get_all_portfolio_value(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    pv_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(folder,file_)
            pv = process_pv(data)
            #if type(div) != 'NonType':
            if len(pv) !=0:
                pv_list.append(pv)
    if len(pv_list) == 0:
        pv = pd.DataFrame()
    else:    
        pv = pd.concat(pv_list,sort=False )
    return pv

def get_all_DW_In_Base(folder):
    #Creates a list of all files in folder
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    depAndWith_list = []
    #Iterates through all files in filelist
    for file_ in filelist:
        #Checks if file is a .csv
        if file_.lower().endswith('.csv'):
            data = convert_to_df(folder,file_)
            depAndWith = process_DW_In_Base(data)
            #if type(div) != 'NonType':
            if len(depAndWith) !=0:
                depAndWith_list.append(depAndWith)
    if len(depAndWith_list) == 0:
        deposits_and_withdrawals = pd.DataFrame()
    else:
        deposits_and_withdrawals = pd.concat(depAndWith_list,sort=False )
    return deposits_and_withdrawals



def calculate_PL (df):
    df['Proceeds'] = df['Proceeds'].astype('float')
    df['Quantity'] = df['Quantity'].astype('float')
    df['T. Price'] = df['T. Price'].astype('float')
    df['Comm/Fee'] = df['Comm/Fee'].astype('float')    
    df['Quantity_Rsum'] =  df.groupby(['Symbol'])['Quantity'].transform(lambda x : x.cumsum())

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
    if len(df) > 0:
        df.drop(['AvgOpenPrice','PL','CumPL'], axis=1, inplace = True, errors= 'ignore')
        dfgrouped = df.groupby('Symbol')
        dataframe_list = []
        for name, group in dfgrouped:
            symbol_group = dfgrouped.get_group(name)
            profLoss = calculate_PL( symbol_group)
            symbol_group = symbol_group.join(profLoss)
            dataframe_list.append(symbol_group)
        new_trades = pd.concat(dataframe_list,sort=False ).sort_index()
    else:
        new_trades = pd.DataFrame()
    return new_trades

def update_symbol_names(folder):
    
    #Read current Symbolkey table if exists or empty dataframe
    if (folder/'tables_symbol_key.xlsx').is_file():
        symbolKey = pd.read_excel(folder/'tables_symbol_key.xlsx')
    else:
        symbolKey = pd.DataFrame()

    #Creates a new dataframe with all new symbol keys
    trades = pd.read_csv(folder/'tables_trades.csv') 
    trades.drop(columns=['Quantity','Currency','Date/Time','T. Price','Proceeds','Comm/Fee','Quantity_Rsum','AvgOpenPrice','PL','CumPL'],axis= 1, inplace=True, errors='ignore')
    trades['Symbol'] = trades.Symbol.str.strip()
    trades['Underliying'] = trades['Symbol'].str.split(n=1).str[0].str.strip()
    trades = trades.drop_duplicates('Symbol').sort_values(by=['Symbol'])
    trades = trades.reset_index(drop=True)

    #Finds the new Symbols
    df_diff = pd.concat([symbolKey,trades], sort=False).drop_duplicates(keep=False)

    # Appends them, sort and reset index
    symbolKey = symbolKey.append(df_diff)
    symbolKey.sort_values(by=['Symbol'],inplace=True)
    symbolKey = symbolKey.reset_index(drop=True)

    return symbolKey

def calculateTWR(amounts):
    ###Returns TWR of amount series
    ##Period return = amount - amount previous period / amount previous period
    #TWR = (Cummulative product of Period returns +1 ) - 1
    # Times 100 as a percentage
    # #df['TWR'] = ((df['PeriodReturn']+1).cumprod()-1)*100
    #amount (series)
    #cash (optional) (series)
    ###
    twr = ((( ((amounts- amounts.shift())/amounts.shift()))+1).cumprod()-1)*100
    return twr


def calculate_periodReturn(amounts, cash=0):
    ###Calulate Period Return
    #amount (series)
    #cash (optional) (series)
    ###
    pr = (amounts - (amounts.shift() + cash))/(amounts.shift() + cash)
    return pr
   