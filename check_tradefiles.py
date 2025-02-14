import pandas as pd
import re
import os
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*truth value of an empty array is ambiguous.*")
pd.set_option('display.max_columns', None)

today = datetime.now().date()
# today = pd.to_datetime('20241202')
# # drop_pattern = rf'dropcopy_(MAIN|main|TEAM|team|BACKUP|backup)_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx' # sample = dropcopy_positions_20241107_165710
# our_pattern = rf'Output_{today.strftime("%d-%b-%y")} \d{{2}}-\d{{2}}-\d{{2}}\.xlsx' # sample = Output_07-Nov-24 15-34-07.xlsx

root_dir = os.getcwd()
our_file_path = os.path.join(root_dir, 'Our_file')
their_file_path = os.path.join(root_dir, 'Their_file')

dir_list = [our_file_path, their_file_path]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

def convert_to_timestamp(date_str):
    date_str = date_str.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '')
    try:
        return pd.to_datetime(date_str, format='%Y%B%d')
    except ValueError:
        return pd.NaT

server_list = ['main', 'backup', 'team']


print('\n')
for each_server in server_list:
    i = 0
    flag = True
    mismatch = 0
    missing_trade = 0
    no_trade = False
    print(f'Server: {each_server.upper()}')
    our_pattern = rf'net_positions_({each_server.lower()}|{each_server.upper()})_{today.strftime("%Y%m%d")}\.csv'  # sample = net_positions_BACKUP_20241202.csv
    drop_pattern = rf'dropcopy_({each_server.lower()}|{each_server.upper()}|{each_server.capitalize()})_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx'  # sample = dropcopy_positions_20241107_165710
    # ---------------------------------------------------------------------------------------------------
    drop_matched_files = [f for f in os.listdir(their_file_path) if re.match(drop_pattern, f)]
    if len(drop_matched_files) == 0:
        print(f'Please download today\'s dropcopy file for Server: {each_server}')
        break
    df_drop = pd.DataFrame()
    for each_file in drop_matched_files:
        temp_df = pd.read_excel(os.path.join(their_file_path, each_file), index_col=False)
        df_drop = pd.concat([df_drop, temp_df])
    # df_drop = pd.read_excel(r'D:\trade_file_analysis\dropcopy_positions_20241105_201400.xlsx', index_col=False)
    our_matched_files = [f for f in os.listdir(our_file_path) if re.match(our_pattern, f)]
    if len(our_matched_files) == 0:
        print(f'Please download today\'s net positions file for Server: {each_server}')
        break
    df_output = pd.DataFrame()
    for each_file in our_matched_files:
        temp_df = pd.read_csv(os.path.join(our_file_path, each_file), index_col=False)
        df_output = pd.concat([df_output, temp_df])
    # df_output = pd.read_excel(r'D:\trade_file_analysis\Output_06-Nov-24 15-43-26.xlsx', index_col=False)
    # df_output = df_output[~df_output['Source1'].str.startswith('Nest')]
    # df_output.rename(
    #     columns={'InstType': 'Option_Type', 'StrikePrice': 'Strike'},
    #     inplace=True)
    # ---------------------------------------------------------------------------------------------------
    df_drop = df_drop.iloc[:, 1:]
    df_drop['Expiry'] = df_drop['Expiry'].apply(convert_to_timestamp)
    df_output['Expiry'] = df_output['Expiry'].apply(convert_to_timestamp)
    # ---------------------------------------------------------------------------------------------------
    filtered_df = pd.DataFrame()
    drop_strikes = df_drop.StrikePrice.unique()
    op_strikes = df_output.StrikePrice.unique()
    if len(op_strikes) == len(drop_strikes) == 0:
        no_trade = True
        print(f'No trade found for server: {each_server}\n')
        continue
    elif len(op_strikes) <= len(drop_strikes):
        filtered_df = df_drop.copy()
        use = 'net_pos'
        # print('missing trade in net position file')
    else:
        filtered_df = df_output.copy()
        use = 'drop'
        # print('No missing trades')

    # # filtered_df = df_output.query("Source1.str.contains(@each_server, case = False, na = False)")
    # filtered_df = df_drop.copy()
    grouped_df = filtered_df.groupby(['Symbol', 'Expiry', 'InstType'])['StrikePrice'].unique().reset_index()
    # ---------------------------------------------------------------------------------------------------
    for index, row in grouped_df.iterrows():
        # print(index)
        # print(type(row))
        symbol = row['Symbol']
        expiry = row['Expiry']
        opttype = row['InstType']
        for each_strike in row['StrikePrice']:
            # print(symbol, expiry, opttype, each_strike)
            # ---------------------------------------------------------------------------------------------------
            if use == 'drop':
                temp_drop_df = df_drop.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_op_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                compare_strikes = df_drop.StrikePrice.unique()
            else:
                temp_op_df = df_output.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_drop_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                compare_strikes = df_output.StrikePrice.unique()
            # ---------------------------------------------------------------------------------------------------
            # # if each_strike in df_output.StrikePrice.unique():
            # temp_op_df = df_output.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
            # temp_drop_df = filtered_df.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
            # # print(temp_op_df)

            if each_strike in compare_strikes:
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

                if op_sell_qty != drop_sell_qty or abs(op_sell_price - drop_sell_price) >= 1 or abs(op_buy_price - drop_buy_price) >= 1 or op_buy_qty != drop_buy_qty:
                    if mismatch == 0:
                        print('Mismatch')
                        mismatch = 1

                if op_sell_qty != drop_sell_qty:
                    i += 1
                    print(
                        f'{i}. Sell Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell qty is not matching with the dropcopy sell qty\nour_sell_qty:{op_sell_qty}, dropcopy_sell_qty:{drop_sell_qty}\n')
                    flag = False

                if abs(op_sell_price - drop_sell_price) >= 1:
                    i += 1
                    print(
                        f'{i}. Sell Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell price is not matching with the dropcopy sell price\nour_sell_price:{op_sell_price}, dropcopy_sell_price:{drop_sell_price[0]}\n')
                    flag = False

                if abs(op_buy_price - drop_buy_price) >= 1:
                    i += 1
                    print(
                        f'{i}. Buy Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy price is not matching with the dropcopy buy price\nour_buy_price:{op_buy_price}, dropcopy_buy_price:{drop_buy_price[0]}\n')
                    flag = False

                if op_buy_qty != drop_buy_qty:
                    i += 1
                    print(
                        f'{i}. Buy Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy qty is not matching with the dropcopy buy qty\nour_buy_qty:{op_buy_qty}, dropcopy_buy_qty:{drop_buy_qty}\n')
                    flag = False
            else:
                temp_op_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                op_buy_qty = sum(temp_op_df['BuyQty'])
                op_buy_price = temp_op_df['BuyPrice'].values[0]
                op_sell_qty = sum(temp_op_df['SellQty'])
                op_sell_price = temp_op_df['SellPrice'].values[0]
                temp_drop_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                drop_buy_qty = sum(temp_drop_df['BuyQty'])
                drop_buy_price = temp_drop_df['BuyPrice'].tolist()
                drop_sell_qty = sum(temp_drop_df['SellQty'])
                drop_sell_price = temp_drop_df['SellPrice'].tolist()
                if use == 'net_pos':
                    print(f'Trade missing in Net Position but present in DropCopy.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {op_buy_qty}, {op_buy_price}\n Sell(Qty,Price): {op_sell_qty}, {op_sell_price}\n')
                    missing_trade = 1
                else:
                    print(f'Trade present in Net Position but missing in DropCopy.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {drop_buy_qty}, {drop_buy_price[0]}\n Sell(Qty,Price): {drop_sell_qty}, {drop_sell_price[0]}\n')
                    missing_trade = 1


    if flag:
        if not missing_trade and not no_trade:
            print(f'No mismatch between the dropcopy and inhouse consolidated trade (net positions) file\n')
        elif not no_trade:
            print(f'\nFor rest of the trades in {each_server}, No mismatch between the dropcopy and inhouse consolidated trade (net positions) file\n')


# if flag:
#     print('No mismatch between the dropcopy and inhouse consolidated trade file')

