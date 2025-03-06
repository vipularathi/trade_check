import pandas as pd
import re
import os
import glob
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*truth value of an empty array is ambiguous.*")
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda a:'%.2f' %a)

today = datetime.now().date()
# today = pd.to_datetime('20250213').date()
# # drop_pattern = rf'dropcopy_(MAIN|main|TEAM|team|BACKUP|backup)_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx' # sample = dropcopy_positions_20241107_165710
# our_pattern = rf'Output_{today.strftime("%d-%b-%y")} \d{{2}}-\d{{2}}-\d{{2}}\.xlsx' # sample = Output_07-Nov-24 15-34-07.xlsx

root_dir = os.getcwd()
our_file_path = os.path.join(root_dir, 'Our_file')
their_file_path = os.path.join(root_dir, 'Their_file')

dir_list = [our_file_path, their_file_path]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]


def convert_to_timestamp(date_str):
    date_str = date_str.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '').replace(' ','')
    try:
        return pd.to_datetime(date_str, format='%Y%B%d')
    except ValueError:
        return pd.NaT


server_list = ['main', 'backup', 'team', 'INHOUSEALGO_NETPOSITION']
# server_name = "INHOUSEALGO_NETPOSITION"
# our_file_path1 = r"W:\Options & Futures Data\trade_check\Our_file"
# dropcopy_file_path1 = r"W:\Options & Futures Data\trade_check\Their_file"

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
    inhouse_pattern = rf'^(dc_)?[Ii]nhouse_algo_{today.strftime("%Y%m%d")}.csv' # sample = Inhouse_algo_20250212.csv or dc_inhouse_algo_20250212.csv
    # ====================================================================================================
    if each_server != 'INHOUSEALGO_NETPOSITION':
        drop_matched_files = [f for f in os.listdir(their_file_path) if re.match(drop_pattern, f)]
        our_matched_files = [f for f in os.listdir(our_file_path) if re.match(our_pattern, f)]
        if len(drop_matched_files) == 0 or len(our_matched_files) == 0:
            # print(f'Please download today\'s dropcopy file for Server: {each_server}')
            print(f'No trade found for Server: {each_server}\n')
            continue
        df_drop = pd.DataFrame()
        for each_file in drop_matched_files:
            temp_df = pd.read_excel(os.path.join(their_file_path, each_file), index_col=False)
            df_drop = pd.concat([df_drop, temp_df])
            df_drop = df_drop.iloc[:, 1:]
            df_drop['Expiry'] = df_drop['Expiry'].apply(convert_to_timestamp)  # sample=2025February27th
        # ---------------------------------------------------------------------------------------------------
        # our_matched_files = [f for f in os.listdir(our_file_path) if re.match(our_pattern, f)]
        # if len(our_matched_files) == 0:
        #     print(f'Please download today\'s net positions file for Server: {each_server}')
        #     break
        df_output = pd.DataFrame()
        for each_file in our_matched_files:
            temp_df = pd.read_csv(os.path.join(our_file_path, each_file), index_col=False)
            df_output = pd.concat([df_output, temp_df])
            df_output['Expiry'] = df_output['Expiry'].apply(convert_to_timestamp)  # sample=2025February27th
    # ---------------------------------------------------------------------------------------------------
    else:
        drop_inhouse_matched_files = [f for f in os.listdir(our_file_path) if re.match(inhouse_pattern, f)]
        our_inhouse_matched_files = [f for f in os.listdir(their_file_path) if re.match(inhouse_pattern, f)]
        if len(our_inhouse_matched_files) == 0 or len(drop_inhouse_matched_files) == 0:
            print(f'No trades to match b/w DC net qty and InhouseAlgo net qty\n')
            break
        df_drop_inhouse = pd.DataFrame()
        for each_file in drop_inhouse_matched_files:
            temp_df = pd.read_csv(os.path.join(our_file_path, each_file), index_col=False)
            df_drop_inhouse = pd.concat([df_drop_inhouse, temp_df])
            df_drop_inhouse.Expiry = pd.to_datetime(df_drop_inhouse.Expiry)  # sample=27-02-2025
        # ---------------------------------------------------------------------------------------------------
        # drop_inhouse_matched_files = [f for f in os.listdir(their_file_path) if re.match(inhouse_pattern, f)]
        # if len(drop_inhouse_matched_files) == 0:
        #     print(f'Please download today\'s inhouse net qty file from DC NET Quantity portal')
        #     break
        df_our_inhouse = pd.DataFrame()
        for each_file in our_inhouse_matched_files:
            temp_df = pd.read_csv(os.path.join(their_file_path, each_file), index_col=False)
            df_our_inhouse = pd.concat([df_our_inhouse, temp_df])
            df_our_inhouse.rename(columns={'buyPrice': 'BuyPrice', 'sellPrice': 'SellPrice'}, inplace=True)
            if 'nan' == str(df_our_inhouse.Exchange.unique()[0]):
                df_our_inhouse = df_our_inhouse.iloc[0:0]
            df_our_inhouse.Expiry = df_our_inhouse.Expiry.apply(convert_to_timestamp)  # sample=2025 February 27th
            col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                        'NetQty']
            df_our_inhouse = df_our_inhouse[col_keep]
    # ====================================================================================================




    # ---------------------------------------------------------------------------------------------------
    filtered_df = pd.DataFrame()
    # drop_strikes = df_drop.StrikePrice.unique()
    # op_strikes = df_output.StrikePrice.unique()
    # drop_inhouse_strikes = df_drop_inhouse.StrikePrice.unique()
    # our_inhouse_strikes = df_our_inhouse.StrikePrice.unique()

    if each_server != 'INHOUSEALGO_NETPOSITION' and len(df_output) == len(df_drop) == 0:
        no_trade = True
        print(f'No trade found for server: {each_server}\n')
        continue
    elif each_server != 'INHOUSEALGO_NETPOSITION' and len(df_output) <= len(df_drop):
        filtered_df = df_drop.copy()
        use = 'net_pos'
        # print('missing trade in net position file')
    elif each_server != 'INHOUSEALGO_NETPOSITION' and len(df_output) >= len(df_drop):
        filtered_df = df_output.copy()
        use = 'drop'
        # print('No missing trades')
    elif len(df_drop_inhouse) == len(df_our_inhouse) == 0 and each_server == 'INHOUSEALGO_NETPOSITION':
        no_trade_inhouse = True
        print(f'No trade found for server: {each_server}\n')
        continue
    elif len(df_our_inhouse) <= len(df_drop_inhouse) and each_server == 'INHOUSEALGO_NETPOSITION':
        filtered_df = df_drop_inhouse.copy()
        use = 'our_inhouse'
    elif len(df_our_inhouse) >= len(df_drop_inhouse) and each_server == 'INHOUSEALGO_NETPOSITION':
        filtered_df = df_our_inhouse.copy()
        use = 'drop_inhouse'

    grouped_df = filtered_df.groupby(['Symbol', 'Expiry', 'InstType'])['StrikePrice'].unique().reset_index()
    # ---------------------------------------------------------------------------------------------------
    for index, row in grouped_df.iterrows():
        # print(index)
        # print(type(row))
        symbol = row['Symbol']
        expiry = row['Expiry']
        opttype = row['InstType']
        for each_strike in row['StrikePrice']:
            # ---------------------------------------------------------------------------------------------------
            if use == 'drop':
                temp_drop_df = df_drop.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_op_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                # compare_strikes = df_drop.StrikePrice.unique()
                compare_strike = temp_drop_df.StrikePrice.unique()
                compare_insttype = temp_drop_df.InstType.unique()
            elif use == 'net_pos':
                temp_op_df = df_output.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_drop_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                # compare_strikes = df_output.StrikePrice.unique()
                compare_strike = temp_op_df.StrikePrice.unique()
                compare_insttype = temp_op_df.InstType.unique()
            elif use == 'drop_inhouse':
                temp_drop_df = df_drop_inhouse.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_op_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                # compare_strikes = df_drop_inhouse.StrikePrice.unique()
                compare_strike = temp_drop_df.StrikePrice.unique()
                compare_insttype = temp_drop_df.InstType.unique()
            elif use == 'our_inhouse':
                temp_op_df = df_our_inhouse.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_drop_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                # compare_strikes = df_our_inhouse.StrikePrice.unique()
                compare_strike = temp_op_df.StrikePrice.unique()
                compare_insttype = temp_op_df.InstType.unique()
            # ---------------------------------------------------------------------------------------------------
            if each_strike in compare_strike and opttype in compare_insttype:
                op_buy_qty = sum(temp_op_df['BuyQty'])
                drop_buy_qty = sum(temp_drop_df['BuyQty'])
                op_buy_price = temp_op_df['BuyPrice'].values[0]
                drop_buy_price = temp_drop_df['BuyPrice'].tolist()
                # if len(temp_op_df['BuyQty']) > 1 or len(temp_op_df['BuyPrice']) > 1:
                #     print('a')
                temp_op_df.loc[:, 'buy_value'] = temp_op_df['BuyQty'] * temp_op_df['BuyPrice']
                op_buy_value = temp_op_df['buy_value'].sum()
                temp_drop_df.loc[:, 'buy_value'] = temp_drop_df['BuyQty'] * temp_drop_df['BuyPrice']
                drop_buy_value = temp_drop_df['buy_value'].sum()

                op_sell_qty = sum(temp_op_df['SellQty'])
                drop_sell_qty = sum(temp_drop_df['SellQty'])
                op_sell_price = temp_op_df['SellPrice'].values[0]
                drop_sell_price = temp_drop_df['SellPrice'].tolist()
                # if len(temp_op_df['SellQty']) > 1 or len(temp_op_df['SellPrice']) > 1:
                #     print('a')
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
                        f'{i}. Sell Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell qty is not matching with the dropcopy sell qty\ndropcopy_sell_qty:{drop_sell_qty}, our_sell_qty:{op_sell_qty}, difference:{abs(op_sell_qty-drop_sell_qty)}\n')
                    flag = False

                if abs(op_sell_price - drop_sell_price) >= 1:
                    i += 1
                    print(
                        f'{i}. Sell Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell price is not matching with the dropcopy sell price\ndropcopy_sell_price:{drop_sell_price[0]}, our_sell_price:{op_sell_price}, difference:{abs(drop_sell_price[0]-op_sell_price)}\n')
                    flag = False

                if abs(op_buy_price - drop_buy_price) >= 1:
                    i += 1
                    print(
                        f'{i}. Buy Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy price is not matching with the dropcopy buy price\ndropcopy_buy_price:{drop_buy_price[0]}, our_buy_price:{op_buy_price}, difference:{abs(drop_buy_price[0]-op_buy_price)}\n')
                    flag = False

                if op_buy_qty != drop_buy_qty:
                    i += 1
                    print(
                        f'{i}. Buy Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy qty is not matching with the dropcopy buy qty\ndropcopy_buy_qty:{drop_buy_qty}, our_buy_qty:{op_buy_qty}, difference:{abs(drop_buy_qty-op_buy_qty)}\n')
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
                elif use == 'drop':
                    print(f'Trade present in Net Position but missing in DropCopy.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {drop_buy_qty}, {drop_buy_price[0]}\n Sell(Qty,Price): {drop_sell_qty}, {drop_sell_price[0]}\n')
                    missing_trade = 1
                elif use == 'our_inhouse':
                    print(f'Trade missing in Inhouse Algo Net Positions but present in DC Net Quantity.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {op_buy_qty}, {op_buy_price}\n Sell(Qty,Price): {op_sell_qty}, {op_sell_price}\n')
                    missing_trade = 1
                elif use == 'drop_inhouse':
                    print(f'Trade present in Inhouse Algo Net Positions but missing in DC Net Quantity.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {drop_buy_qty}, {drop_buy_price[0]}\n Sell(Qty,Price): {drop_sell_qty}, {drop_sell_price[0]}\n')
                    missing_trade = 1

    if flag:
        if not missing_trade and not no_trade and each_server != 'INHOUSEALGO_NETPOSITION':
            print(f'No mismatch between the dropcopy and inhouse consolidated trade (net positions) file\n')
        elif not no_trade and each_server != 'INHOUSEALGO_NETPOSITION':
            print(f'\nFor rest of the trades in {each_server}, No mismatch between the dropcopy and inhouse consolidated trade (net positions) file\n')
        elif not missing_trade and not no_trade and each_server == 'INHOUSEALGO_NETPOSITION':
            print(f'No mismatch between the DC Net Quantity and Inhouse Algo Net Positions file\n')
        elif not no_trade and each_server == 'INHOUSEALGO_NETPOSITION':
            print(f'\nFor rest of the trades in {each_server}, No mismatch between the DC Net Quantity and Inhouse Algo Net Positions file\n')
