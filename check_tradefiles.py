import pandas as pd
import re
import os
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
# warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*truth value of an empty array is ambiguous.*")
pd.set_option('display.max_columns', None)

today = datetime.now().date()
# today = pd.to_datetime('20241108')
# drop_pattern = rf'dropcopy_(MAIN|main|TEAM|team|BACKUP|backup)_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx' # sample = dropcopy_positions_20241107_165710
our_pattern = rf'Output_{today.strftime("%d-%b-%y")} \d{{2}}-\d{{2}}-\d{{2}}\.xlsx' # sample = Output_07-Nov-24 15-34-07.xlsx

root_dir = os.getcwd()
our_file_path = os.path.join(root_dir, 'Our_file')
their_file_path = os.path.join(root_dir, 'Their_file')

matched_files = [f for f in os.listdir(our_file_path) if re.match(our_pattern, f)]
df_output = pd.DataFrame()
for each_file in matched_files:
    temp_df = pd.read_excel(os.path.join(our_file_path, each_file), index_col=False)
    df_output = pd.concat([df_output, temp_df])
# df_output = pd.read_excel(r'D:\trade_file_analysis\Output_06-Nov-24 15-43-26.xlsx', index_col=False)

df_output = df_output[~df_output['Source1'].str.startswith('Nest')]
df_output.rename(columns = {'Instrument Name' : 'Instrument_Name', 'Option Type': 'Option_Type', 'Series/Expiry' : 'Expiry'}, inplace=True)

def convert_to_timestamp(date_str):
    date_str = date_str.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '')
    try:
        return pd.to_datetime(date_str, format='%Y%B%d')
    except ValueError:
        return pd.NaT

server_list = ['main', 'backup', 'team']

i=0
print('\n')
for each_server in server_list:
    flag = True
    print(f'Server: {each_server.upper()}')
    drop_pattern = rf'dropcopy_({each_server.lower()}|{each_server.upper()}|{each_server.capitalize()})_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx'  # sample = dropcopy_positions_20241107_165710
    matched_files = [f for f in os.listdir(their_file_path) if re.match(drop_pattern, f)]
    df_drop = pd.DataFrame()
    for each_file in matched_files:
        temp_df = pd.read_excel(os.path.join(their_file_path, each_file), index_col=False)
        df_drop = pd.concat([df_drop, temp_df])
    # df_drop = pd.read_excel(r'D:\trade_file_analysis\dropcopy_positions_20241105_201400.xlsx', index_col=False)

    df_drop = df_drop.iloc[:, 1:]
    df_drop['Expiry'] = df_drop['Expiry'].apply(convert_to_timestamp)

    filtered_df = df_output.query("Source1.str.contains(@each_server, case = False, na = False)")
    grouped_df = filtered_df.groupby(['Symbol', 'Expiry', 'Option_Type'])['Strike'].unique().reset_index()

    for index, row in grouped_df.iterrows():
        # print(index)
        # print(type(row))
        symbol = row['Symbol']
        expiry = row['Expiry']
        opttype = row['Option_Type']
        for each_strike in row['Strike']:
            # print(symbol, expiry, opttype, each_strike)
            temp_op_df = filtered_df.query("Symbol == @symbol and Expiry == @expiry and Option_Type == @opttype and Strike == @each_strike")
            temp_drop_df = df_drop.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
            # print(temp_op_df)

            op_buy_qty = sum(temp_op_df['BuyQty'])
            drop_buy_qty = sum(temp_drop_df['BuyQty'])
            op_buy_price = temp_op_df['BuyPrice'].values[0]
            drop_buy_price = temp_drop_df['BuyPrice'].tolist()
            if len(temp_op_df['BuyQty']) > 1 or len(temp_op_df['BuyPrice']) > 1:
                print('a')
            temp_op_df.loc[:, 'buy_value'] = temp_op_df['BuyQty'] * temp_op_df['BuyPrice']
            op_buy_value = temp_op_df['buy_value'].sum()
            temp_drop_df.loc[:, 'buy_value'] = temp_drop_df['BuyQty'] * temp_drop_df['BuyPrice']
            drop_buy_value = temp_drop_df['buy_value'].sum()

            op_sell_qty = sum(temp_op_df['SellQty'])
            drop_sell_qty = sum(temp_drop_df['SellQty'])
            op_sell_price = temp_op_df['SellPrice'].values[0]
            drop_sell_price = temp_drop_df['SellPrice'].tolist()
            if len(temp_op_df['SellQty']) > 1 or len(temp_op_df['SellPrice']) > 1:
                print('a')
            temp_op_df.loc[:, 'sell_value'] = temp_op_df['SellQty'] * temp_op_df['SellPrice']
            op_sell_value = temp_op_df['sell_value'].sum()
            temp_drop_df.loc[:, 'sell_value'] = temp_drop_df['SellQty'] * temp_drop_df['SellPrice']
            drop_sell_value = temp_drop_df['sell_value'].sum()

            if op_sell_qty != drop_sell_qty:
                i += 1
                print(
                    f'{i}. Sell Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell qty is not matching with the dropcopy sell qty\ndropcopy_sell_qty:{drop_sell_qty}, our_sell_qty:{op_sell_qty}\n')
                flag = False

            if op_sell_price != drop_sell_price:
                i += 1
                print(
                    f'{i}. Sell Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell price is not matching with the dropcopy sell price\ndropcopy_sell_price:{drop_sell_price}, our_sell_price:{op_sell_price}\n')
                flag = False

            # if op_sell_value != drop_sell_value:
            #     i+=1
            #     print(
            #         f'{i}. Sell Value Mismatch: {each_server.upper()}, {symbol}, {expiry.date()}, {opttype}, {each_strike}\ndropcopy_sell_value:{drop_sell_value}, our_sell_value:{op_sell_value}\n')
            #     flag = False

            if op_buy_price!= drop_buy_price:
                i += 1
                print(
                    f'{i}. Buy Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy price is not matching with the dropcopy buy price\ndropcopy_buy_price:{drop_buy_price}, our_buy_price:{op_buy_price}\n')
                flag = False

            if op_buy_qty != drop_buy_qty:
                i += 1
                print(
                    f'{i}. Buy Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy qty is not matching with the dropcopy buy qty\ndropcopy_buy_qty:{drop_buy_qty}, our_buy_qty:{op_buy_qty}\n')
                flag = False

            # if op_buy_value != drop_buy_value:
            #     i += 1
            #     print(
            #         f'{i}. Buy Value Mismatch: {each_server.upper()}, {symbol}, {expiry.date()}, {opttype}, {each_strike}\ndropcopy_buy_value:{drop_buy_value}, our_buy_value:{op_buy_value}\n')
            #     flag = False

    if flag:
        print(f'No mismatch between the dropcopy and inhouse consolidated trade file\n')


# if flag:
#     print('No mismatch between the dropcopy and inhouse consolidated trade file')

