import requests
import os
import pandas as pd
import warnings

warnings.filterwarnings("ignore")
root_dir = os.getcwd()
combined_dir = os.path.join(root_dir,'Combined_files')
os.makedirs(combined_dir, exist_ok=True)

base_url = 'http://172.16.47.87:5000/download/'
endpoint_filename_server_dict = {
    'combined_net':['combined_net_position.csv','','COMBINED_NETPOSITION'],
    'nest_bse_net':['nse_nest_net_position.csv','Nest-TradeHist','NSE_Nest_Netposition'],
    'nest_nse_net':['bse_nest_net_position.csv','BSE_trades','BSE_Nest_Netposition'],
    'Inhouse_algo':['inhouse_algo_net_position.csv','Inhouse_algo','Inhouse_Algo_Netposition'],
    'main_dev':['main_net_position.csv','Algo_main_demo','Main_Netposition'],
    'backup':['backup_net_position.csv','Algo_backup','Backup_Netposition']
}
key_list = list(endpoint_filename_server_dict.keys())
# filename_dict = 'inhouse_test.csv'
a=0
# for key, value in endpoint_filename_server_dict.items():
#     download_url = base_url+key
#     response= requests.get(url=download_url, stream=True)
#     combined_file_path = os.path.join(combined_dir, value)
#
#     if response.status_code == 200:
#         with open(combined_file_path, 'wb') as file:
#             for chunk in response.iter_content(chunk_size=1024):
#                 file.write(chunk)
#         print(f'file: {value} downloaded successfully')
#
#     else:
#         print(f'Error: Failed to download file. Status code: {response.status_code}')
b=0
print('\n')
print(f'Server: COMBINED  NET POSITION')
download_url = base_url+key_list[0]
df_combined = pd.read_csv(download_url)
df_combined.columns = df_combined.columns.str.replace(' ','')
df_combined.rename(columns={'Series/Expiry':'Expiry', 'Strike':'StrikePrice', 'OptionType':'InstType'}, inplace=True)
for each in key_list[1:]:
    mismatch = 0
    missing_trade = 0
    no_trade = False
    i=0
    flag=True
    download_url = base_url+each
    df_each = pd.read_csv(download_url)
    # print(each,'\n',df_each)
    source1 = endpoint_filename_server_dict[each][1]
    # print(f'source1 is {source1}')
    df_extract = df_combined.query("Source1 == @source1")
    if len(df_each) == len(df_extract) == 0:
        no_trade = True
        print(f'No trade found for server: {endpoint_filename_server_dict[each][2]}\n')
        continue
    elif len(df_each) <= len(df_extract):
        filtered_df = df_extract.copy()
        use = 'each'
    else:
        filtered_df = df_each.copy()
        use = 'extract'

    # drop=extract,         output=each

    grouped_df = filtered_df.groupby(by=['Symbol', 'Expiry', 'InstType'])['StrikePrice'].unique().reset_index()
    for index, row in grouped_df.iterrows():
        symbol = row['Symbol']
        expiry = row['Expiry']
        opttype = row['InstType']
        for each_strike in row['StrikePrice']:
            if use == 'extract':
                temp_drop_df = df_extract.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_op_df = filtered_df.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                compare_strike = temp_drop_df.StrikePrice.unique()
                compare_insttype = temp_drop_df.InstType.unique()
            elif use == 'each':
                temp_op_df = df_each.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                temp_drop_df = filtered_df.query(
                    "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                compare_strike = temp_op_df.StrikePrice.unique()
                compare_insttype = temp_op_df.InstType.unique()

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

                if op_sell_qty != drop_sell_qty or abs(op_sell_price - drop_sell_price) >= 1 or abs(
                        op_buy_price - drop_buy_price) >= 1 or op_buy_qty != drop_buy_qty:
                    if mismatch == 0:
                        print('Mismatch')
                        mismatch = 1

                if op_sell_qty != drop_sell_qty:
                    i += 1
                    print(f'{i}. Sell Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell qty is not matching with the dropcopy sell qty\nour_sell_qty:{op_sell_qty}, dropcopy_sell_qty:{drop_sell_qty}\n')
                    flag = False

                if abs(op_sell_price - drop_sell_price) >= 1:
                    i += 1
                    print(f'{i}. Sell Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur sell price is not matching with the dropcopy sell price\nour_sell_price:{op_sell_price}, dropcopy_sell_price:{drop_sell_price[0]}\n')
                    flag = False

                if abs(op_buy_price - drop_buy_price) >= 1:
                    i += 1
                    print(f'{i}. Buy Price Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy price is not matching with the dropcopy buy price\nour_buy_price:{op_buy_price}, dropcopy_buy_price:{drop_buy_price[0]}\n')
                    flag = False

                if op_buy_qty != drop_buy_qty:
                    i += 1
                    print(f'{i}. Buy Quantity Mismatch: {symbol}, {expiry.date()}, {opttype}, {each_strike}\nOur buy qty is not matching with the dropcopy buy qty\nour_buy_qty:{op_buy_qty}, dropcopy_buy_qty:{drop_buy_qty}\n')
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
                if use == 'each':
                    print(
                        f'Trade missing in {endpoint_filename_server_dict[each][2]} file but present in combined file.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {op_buy_qty}, {op_buy_price}\n Sell(Qty,Price): {op_sell_qty}, {op_sell_price}\n')
                    missing_trade = 1
                elif use == 'extract':
                    print(
                        f'Trade present in {endpoint_filename_server_dict[each][2]} file but missing in combined file.\n{symbol}, {expiry.date()}, {opttype}, {each_strike}\n Buy(Qty,Price): {drop_buy_qty}, {drop_buy_price[0]}\n Sell(Qty,Price): {drop_sell_qty}, {drop_sell_price[0]}\n')
                    missing_trade = 1
    if flag:
        if not missing_trade and not no_trade:
            print(f'No mismatch between the combined and {endpoint_filename_server_dict[each][2]} file\n')
        elif not no_trade:
            print(
                f'\nFor rest of the trades in {endpoint_filename_server_dict[each][2]}, No mismatch between the combined and inhouse {each} net positions file\n')
c=0