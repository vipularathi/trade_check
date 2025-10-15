import requests, os, re, warnings
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from prettytable import PrettyTable
from io import StringIO

# os.environ['HTTP_PROXY'] = ''
# os.environ['HTTPS_PROXY'] = ''
pd.set_option('display.float_format', lambda a:'%.2f' %a)
pd.set_option('display.max_columns', None)
warnings.filterwarnings("ignore")
root_dir = os.getcwd()
combined_dir = os.path.join(root_dir,'Combined_files')
os.makedirs(combined_dir, exist_ok=True)
test_dir = os.path.join(root_dir,f'testing')
today = datetime.now().date()

their_file_path = os.path.join(root_dir, 'Their_file')
proxies = {"http": None, "https": None}
base_url = 'http://172.16.47.87:5000/download/'
proxies = {'http':None,'https':None}
endpoint_filename_server_dict = {
    # end-point : [filename.ext, Source1, NameToPrint] # filename is not being used #take files outside API folder
    'combined_net':['combined_net_position.csv','','COMBINED_NETPOSITION'],
    'nest_nse_net':['nse_nest_net_position.csv','Nest-TradeHist','NSE_Nest_Netposition'],
    'nest_bse_net':['bse_nest_net_position.csv','BSE_trades','BSE_Nest_Netposition'],
    'Inhouse_algo':['inhouse_algo_net_position.csv','Inhouse_algo','Inhouse_Algo_Netposition'],
    'main_dev':['main_net_position.csv','Algo_main_demo','Main_Netposition'],
    'backup':['backup_net_position.csv','Algo_backup','Backup_Netposition']
}
key_list = list(endpoint_filename_server_dict.keys())

backup_main_url = 'http://172.16.47.87:5000/download/'
team_url = 'http://172.16.47.87:5001/download/'
for_server = ['backup','main_demo','team','algo2_pos','algo41_pos_dc','algo81_pos_dc','algo82_pos_dc','algo66_pos_dc']
route_dict = {
    'dropcopy':['backup','main','team','algo2_pos','algo41_pos_dc','algo81_pos_dc','algo82_pos_dc','algo66_pos_dc'],
    'file_downloader':[{
        'backup':backup_main_url+ 'backup',
        'main':backup_main_url + 'main_dev',
        'team':team_url + 'team',
        'algo2_pos':backup_main_url + 'algo2_pos',
        'algo41_pos_dc':backup_main_url + 'algo41_pos_dc',
        'algo81_pos_dc':backup_main_url + 'algo81_pos_dc',
        'algo82_pos_dc':backup_main_url + 'algo82_pos_dc',
        'algo66_pos_dc':backup_main_url + 'algo66_pos_dc'
    }],
    'api':[{
        'backup':rf"D:\trade_file_analysis\QI_files\API\backup.csv",
        'main':rf"D:\trade_file_analysis\QI_files\API\main_demo.csv",
        'team':rf"D:\trade_file_analysis\QI_files\API\team.csv",
        'algo2_pos':rf"D:\trade_file_analysis\QI_files\API\algo2_pos.csv",
        'algo41_pos_dc':rf"D:\trade_file_analysis\QI_files\API\algo41_pos_dc.csv",
        'algo81_pos_dc':rf"D:\trade_file_analysis\QI_files\API\algo81_pos_dc.csv",
        'algo82_pos_dc':rf"D:\trade_file_analysis\QI_files\API\algo82_pos_dc.csv",
        'algo66_pos_dc':rf"D:\trade_file_analysis\QI_files\API\algo66_pos_dc.csv"
    }]
}

def convert_to_timestamp(date_input):
    if isinstance(date_input, date):
        return date_input
    if isinstance(date_input, str):
        date_str = date_input.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '').replace(' ','')
        date_format = ['%Y%B%d', '%Y-%m-%d', '%d-%m-%Y', '%d%b%Y']
        for each in date_format:
            try:
                return datetime.strptime(date_str, each).date()
            except ValueError:
                continue
    return pd.NaT

new_server_dict = {
    'algo2_pos': 'Colo 68 : BSE',
    'algo41_pos_dc': 'Colo 41 : NSE',
    'algo81_pos_dc': 'Colo 81 : BSE',
    'algo82_pos_dc': 'Colo 82 : NSE',
    'algo66_pos_dc': 'Colo 66 : NSE'
}

col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                    'NetQty']

for each_server in route_dict['dropcopy']:
    if each_server == 'algo82_pos_dc':
        continue
    if each_server in new_server_dict.keys():
        print(f"\nServer: {new_server_dict.get(each_server).upper()}")
    else:
        print(f'\nServer: {each_server.upper()}')
    drop_pattern = rf'dropcopy_({each_server.lower()}|{each_server.upper()}|{each_server.capitalize()})_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx'
    # sample = dropcopy_positions_20241107_165710
    # algo2 sample = dropcopy_algo2_positions_20250702_165710
    # algo41_pos_dc sample = dropcopy_algo41_pos_dc_positions_20250715_16510
    drop_matched_files = [f for f in os.listdir(their_file_path) if re.match(drop_pattern, f)]

    df_drop = pd.DataFrame()
    for each_file in drop_matched_files:
        temp_df = pd.read_excel(os.path.join(their_file_path, each_file), index_col=False)
        df_drop = pd.concat([df_drop, temp_df])
        df_drop.drop(columns = [col for col in df_drop.columns if col not in col_keep], inplace=True)
        df_drop['Expiry'] = df_drop['Expiry'].apply(convert_to_timestamp)  # sample=2025February27th
        df_drop['Expiry'] = pd.to_datetime(df_drop['Expiry'], dayfirst=True).dt.date

    df_api = pd.read_csv(rf"{route_dict['api'][0][each_server]}", index_col=False)
    df_api.drop(columns=[col for col in df_api.columns if col not in col_keep], inplace=True)
    df_api['Expiry'] = df_api['Expiry'].apply(convert_to_timestamp)  # sample=06-03-2025
    df_api['Expiry'] = pd.to_datetime(df_api['Expiry'], dayfirst=True).dt.date

    df_file_downloader = pd.DataFrame()
    resp = requests.get(url=rf"{route_dict['file_downloader'][0][each_server]}", proxies=proxies)
    if resp.status_code != 200:
        if len(df_api) == len(df_drop) == 0:
            if each_server in new_server_dict.keys():
                print(f"No trade found for {new_server_dict.get(each_server).upper()}")
            else:
                print(f'No trade found for {each_server}\n')
            continue
    df_file_downloader = pd.read_csv(StringIO(resp.text))
    df_file_downloader.drop(columns=[col for col in df_file_downloader.columns if col not in col_keep], inplace=True)
    # df_file_downloader = pd.read_csv(rf"{route_dict['file_downloader'][0][each_server]}", index_col=False)
    df_file_downloader.Expiry = df_file_downloader.Expiry.apply(convert_to_timestamp)
    df_file_downloader['Expiry'] = pd.to_datetime(df_file_downloader['Expiry'], dayfirst=True).dt.date

    if len(df_drop) == len(df_api) == len(df_file_downloader) == 0:
        if each_server in new_server_dict.keys():
            print(f"No trade found for {new_server_dict.get(each_server).upper()}")
        else:
            print(f'No trade found for {each_server}\n')
        no_trade = True
        continue
    elif len(df_drop) == 0 and len(df_api) and len(df_file_downloader):
        if each_server in new_server_dict.keys():
            print(f"dropcopy for {new_server_dict.get(each_server).upper()} is empty while data is there in API and "
                  f"file_downloader, hence skipping check.")
        else:
            print(f'dropcopy for {each_server} is empty while data is there in API and file_downloader, hence skipping '
              f'check.')
        continue
    elif len(df_api) == 0 and len(df_drop) and len(df_file_downloader):
        if each_server in new_server_dict.keys():
            print(f"API for {new_server_dict.get(each_server).upper()} is empty while data is there in dropcopy and "
                  f"file_downloader, hence skipping check.")
        else:
            print(f'API for {each_server} is empty while data is there in dropcopy and file_downloader, hence skipping '
              f'check.')
        continue
    elif len(df_file_downloader) == 0 and len(df_api) and len(df_drop):
        if each_server in new_server_dict.keys():
            print(f"File Downloader for {new_server_dict.get(each_server).upper()} is empty while data is there in dropcopy "
                  f"and API, hence skipping check.")
        else:
            print(f'File Downloader for {each_server} is empty while data is there in dropcopy and API, '
              f'hence skipping check.')
        continue
    if each_server in ['algo2_pos','algo41_pos_dc','algo81_pos_dc','algo82_pos_dc','algo66_pos_dc']:
        temp_drop_df = df_drop.copy()
        temp_drop_df['BuyValue'] = temp_drop_df['BuyQty'] * temp_drop_df['BuyPrice']
        temp_drop_df['SellValue'] = temp_drop_df['SellQty'] * temp_drop_df['SellPrice']
        grouped_temp_df = temp_drop_df.groupby(
            by=['Symbol', 'Expiry', 'StrikePrice', 'InstType'], as_index=False).agg(
            {'BuyQty': 'sum', 'SellQty': 'sum', 'NetQty': 'sum', 'BuyValue': 'sum', 'SellValue': 'sum'}
        )
        grouped_temp_df['BuyPrice'] = grouped_temp_df.apply(
            lambda x: x['BuyValue'] / x['BuyQty'] if x['BuyQty'] > 0 else 0, axis=1
        )
        grouped_temp_df['SellPrice'] = np.where(grouped_temp_df['SellQty'] > 0, grouped_temp_df[
            'SellValue'] / grouped_temp_df['SellQty'], 0)
        df_drop = grouped_temp_df.copy()
        df_drop.to_excel(os.path.join(
            test_dir,f'df_drop_{each_server}_{datetime.today().time().strftime("%H%M%S")}.xlsx'),
            index=False
        )
        # =======================================================================================================
        temp_api_df = df_api.copy()
        temp_api_df['BuyValue'] = temp_api_df['BuyQty'] * temp_api_df['BuyPrice']
        temp_api_df['SellValue'] = temp_api_df['SellQty'] * temp_api_df['SellPrice']
        grouped_temp_df = temp_api_df.groupby(
            by=['Symbol', 'Expiry', 'StrikePrice', 'InstType'], as_index=False).agg(
            {'BuyQty':'sum','SellQty':'sum','NetQty':'sum','BuyValue':'sum','SellValue':'sum'}
        )
        grouped_temp_df['BuyPrice'] = grouped_temp_df.apply(
            lambda x: x['BuyValue']/x['BuyQty'] if x['BuyQty'] > 0 else 0, axis=1
        )
        grouped_temp_df['SellPrice'] = np.where(grouped_temp_df['SellQty'] > 0, grouped_temp_df[
            'SellValue']/grouped_temp_df['SellQty'], 0)
        df_api = grouped_temp_df.copy()
        df_api.to_excel(os.path.join(
            test_dir, f'df_api_{each_server}_{datetime.today().time().strftime("%H%M%S")}.xlsx'
        ), index=False)
        # =======================================================================================================
        temp_file_downloader_df = df_file_downloader.copy()
        temp_file_downloader_df['BuyValue'] = temp_file_downloader_df['BuyQty'] * temp_file_downloader_df['BuyPrice']
        temp_file_downloader_df['SellValue'] = temp_file_downloader_df['SellQty'] * temp_file_downloader_df['SellPrice']
        grouped_temp_df = temp_file_downloader_df.groupby(
            by=['Symbol', 'Expiry', 'StrikePrice', 'InstType'], as_index=False).agg(
            {'BuyQty': 'sum', 'SellQty': 'sum', 'NetQty': 'sum', 'BuyValue': 'sum', 'SellValue': 'sum'}
        )
        grouped_temp_df['BuyPrice'] = grouped_temp_df.apply(
            lambda x: x['BuyValue'] / x['BuyQty'] if x['BuyQty'] > 0 else 0, axis=1
        )
        grouped_temp_df['SellPrice'] = np.where(grouped_temp_df['SellQty'] > 0, grouped_temp_df[
            'SellValue'] / grouped_temp_df['SellQty'], 0)
        df_file_downloader = grouped_temp_df.copy()
        df_file_downloader.to_excel(os.path.join(
            test_dir, f'df_file_downloader_{each_server}_{datetime.today().time().strftime("%H%M%S")}.xlsx'
        ), index=False)
    for cntr in range(2):
        if cntr == 0:
            i = 0
            flag = True
            mismatch = 0
            missing_trade = 0
            no_trade = False
            for_print = 'dropcopy'
            print('API data vs dropcopy data:')
        else:
            df_drop = df_file_downloader.copy()
            for_print = 'file downloader'
            i = 0
            flag = True
            mismatch = 0
            missing_trade = 0
            no_trade = False
            print('API data vs file downloader data:')
        if len(df_drop)<len(df_api):
            filtered_df = df_api
            use='drop'
        else:
            filtered_df = df_drop
            use='api'
        grouped_df = filtered_df.groupby(['Symbol', 'Expiry', 'InstType'])['StrikePrice'].unique().reset_index()
        for index, row in grouped_df.iterrows():
            # print(index)
            # print(type(row))
            symbol = row['Symbol']
            expiry = row['Expiry']
            opttype = row['InstType']
            for each_strike in row['StrikePrice']:
                if use == 'drop':
                    temp_drop_df = df_drop.query(
                        "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                    temp_op_df = filtered_df.query(
                        "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                    compare_strike = temp_drop_df.StrikePrice.unique()
                    compare_insttype = temp_drop_df.InstType.unique()
                else:
                    temp_op_df = df_api.query(
                        "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                    temp_drop_df = filtered_df.query(
                        "Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
                    compare_strike = temp_op_df.StrikePrice.unique()
                    compare_insttype = temp_op_df.InstType.unique()

                if each_strike in compare_strike and opttype in compare_insttype:
                    op_buy_qty = sum(temp_op_df['BuyQty'])
                    drop_buy_qty = sum(temp_drop_df['BuyQty'])
                    op_buy_price = temp_op_df['BuyPrice'].values[0]
                    drop_buy_price = temp_drop_df['BuyPrice'].values[0]

                    temp_op_df.loc[:, 'buy_value'] = temp_op_df['BuyQty'] * temp_op_df['BuyPrice']
                    op_buy_value = temp_op_df['buy_value'].sum()
                    temp_drop_df.loc[:, 'buy_value'] = temp_drop_df['BuyQty'] * temp_drop_df['BuyPrice']
                    drop_buy_value = temp_drop_df['buy_value'].sum()

                    op_sell_qty = sum(temp_op_df['SellQty'])
                    drop_sell_qty = sum(temp_drop_df['SellQty'])
                    op_sell_price = temp_op_df['SellPrice'].values[0]
                    drop_sell_price = temp_drop_df['SellPrice'].values[0]

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
                        print(
                            f'{i}. Sell Quantity Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}\nAPI sell qty is not matching with the {for_print} sell qty\n{for_print}_sell_qty:{drop_sell_qty}, api_sell_qty:{op_sell_qty}, difference:{abs(op_sell_qty - drop_sell_qty)}\n')
                        flag = False

                    if abs(op_sell_price - drop_sell_price) >= 1:
                        i += 1
                        print(
                            f'{i}. Sell Price Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}\nAPI sell price is not matching with the {for_print} sell price\n{for_print}_sell_price:{drop_sell_price}, api_sell_price:{op_sell_price}, difference:{abs(drop_sell_price - op_sell_price)}\n')
                        flag = False

                    if abs(op_buy_price - drop_buy_price) >= 1:
                        i += 1
                        print(
                            f'{i}. Buy Price Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}\nAPI buy price is not matching with the {for_print} buy price\n{for_print}_buy_price:{drop_buy_price}, api_buy_price:{op_buy_price}, difference:{abs(drop_buy_price - op_buy_price)}\n')
                        flag = False

                    if op_buy_qty != drop_buy_qty:
                        i += 1
                        print(
                            f'{i}. Buy Quantity Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}\nAPI buy qty is not matching with the {for_print} buy qty\n{for_print}_buy_qty:{drop_buy_qty}, api_buy_qty:{op_buy_qty}, difference:{abs(drop_buy_qty - op_buy_qty)}\n')
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
                    if use == 'drop':
                        print(
                            f'Trade present in API but missing in {for_print}.\n{symbol}, {expiry}, {opttype}, {each_strike}\n Buy(Qty,Price): {drop_buy_qty}, {drop_buy_price[0]}\n Sell(Qty,Price): {drop_sell_qty}, {drop_sell_price[0]}\n')
                        missing_trade = 1
                    else:
                        print(
                            f'Trade present in {for_print} but missing in API.\n{symbol}, {expiry}, {opttype}, {each_strike}\n Buy(Qty,Price): {op_buy_qty}, {op_buy_price}\n Sell(Qty,Price): {op_sell_qty}, {op_sell_price}\n')
                        missing_trade = 1
        if flag:
            if not missing_trade and not no_trade:
                print(f'No mismatch between the {for_print} and API file\n')
            elif not no_trade:
                if each_server in new_server_dict.keys():
                    each_server = new_server_dict.get(each_server).upper()
                print(
                    f'For rest of the trades in {each_server}, No mismatch between the {for_print} and API file\n')

p=0