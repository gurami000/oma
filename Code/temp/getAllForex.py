import pandas as pd
import os
import sys

#Get all Forex
def get_all_forex(folder):
    #Creates a list of all files in folder.
    filelist = os.listdir(folder)
    #Creates a empty list to append all dataframes
    dataframe_list = []
    #Iterates through all files in filelist
    for file in filelist:
        #Checks if file is a .csv
        if file.lower().endswith('.csv'):
            # Creates a relative path from working directory to the current element of filelist
            file_path = os.path.join('IbActivityNew', file)
            # reads file and creates a dataframe forcing 17 columns
            data = pd.read_csv(file_path, names=list('abcdefghijklmnopq'))
            # Filters first column 'Trades' values
            data_trades = data[data.a == 'Trades']
            #Checks if any trades on file
            if len(data_trades) > 0:
                # Dataframe from the first row
                data_headers = data_trades.iloc[0]
                #Uncoment this line and related if necessary to make collumns single word. Comment out equivalent lines
                #data_headers = data_headers.str.replace(' ', '_')
                # Convert dataframe to list
                data_headers = list(data_headers)
                # Assigns values to Coumns           
                data_trades.columns = data_headers
                # Removes first row that now is duplicated
                trades = data_trades.drop(data_trades.index[0])
                #Filters data
                trades = trades[trades.Header == 'Data']
                trades = trades[trades['Asset Category'] == 'Forex']
                #trades = trades[data_trades['Asset_Category'] != 'Forex']
                #Check if any trades after filtering
                if len(trades) > 0:
                    # Remove columns with NaN values
                    trades = trades.dropna(axis=1)
                    #trades = trades.drop(columns=['Trades','Header','DataDiscriminator')
                    # Removes columns
                    delete_columns = ['Trades','Header','DataDiscriminator','Basis','Realized P/L','C. Price','MTM P/L','MTM in USD','Code']
                    for  i in  delete_columns:
                        if i in trades.columns:
                            trades = trades.drop(columns=i)             
                
                    #trades = trades.drop(columns=['Trades','Header','DataDiscriminator'])
                    # Fix some incorrect values on Quantity eg 2,000 -> 2000
                    trades['Quantity'] = trades['Quantity'].str.replace(',', '')
                    trades.rename(columns={'Comm in USD': 'Comm/Fee'}, inplace=True)
                    # Appends to dataframe list
                    dataframe_list.append(trades)
                # Checks if dataframe list is empty    
                if len(dataframe_list) > 0:
                # Concatenates all dataframes
                # Creates a dataframe with all new trades
                    dataframe = pd.concat(dataframe_list,sort=False ).reset_index(drop=True)
                    dataframe['Proceeds'] = dataframe['Proceeds'].astype('float')
                    dataframe['Quantity'] = dataframe['Quantity'].astype('float')
                    dataframe['T. Price'] = dataframe['T. Price'].astype('float')
                    dataframe['Comm/Fee'] = dataframe['Comm/Fee'].astype('float')
                    dataframe['Date/Time'] = pd.to_datetime(dataframe['Date/Time'])
                    dataframe['Quantity_Rsum'] =  dataframe.groupby(['Symbol'])['Quantity'].transform(lambda x : x.cumsum())

    return dataframe