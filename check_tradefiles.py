import pandas as pd
import re
import os
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
pd.set_option('display.max_columns', None)

today = datetime.now().date().strftime('%Y%m%d')
# today = pd.to_datetime('20241107')
drop_pattern = rf'dropcopy_positions_{today.strftime("%Y%m%d")}_\d{{6}}\.xlsx' # sample = dropcopy_positions_20241107_165710
our_pattern = rf'Output_{today.strftime("%d-%b-%y")} \d{{2}}-\d{{2}}-\d{{2}}\.xlsx' # sample = Output_07-Nov-24 15-34-07.xlsx

root_dir = os.getcwd()
our_file_path = os.path.join(root_dir, 'Our_file')
their_file_path = os.path.join(root_dir, 'Their_file')

matched_files = [f for f in os.listdir(their_file_path) if re.match(drop_pattern, f)]
df_drop = pd.DataFrame()
for each_file in matched_files:
    temp_df = pd.read_excel(os.path.join(their_file_path, each_file), index_col=False)
    df_drop = pd.concat([df_drop, temp_df])
# df_drop = pd.read_excel(r'D:\trade_file_analysis\dropcopy_positions_20241105_201400.xlsx', index_col=False)

matched_files = [f for f in os.listdir(our_file_path) if re.match(our_pattern, f)]
df_output = pd.DataFrame()
for each_file in matched_files:
    temp_df = pd.read_excel(os.path.join(our_file_path, each_file), index_col=False)
    df_output = pd.concat([df_output, temp_df])
# df_output = pd.read_excel(r'D:\trade_file_analysis\Output_06-Nov-24 15-43-26.xlsx', index_col=False)

df_drop = df_drop.iloc[:, 1:]
df_output = df_output[~df_output['Source1'].str.startswith('Nest')]
df_output.rename(columns = {'Instrument Name' : 'Instrument_Name', 'Option Type': 'Option_Type', 'Series/Expiry' : 'Expiry'}, inplace=True)
def convert_to_timestamp(date_str):
    # Remove suffixes from the day part
    date_str = date_str.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '')

    # Parse the date with pandas, specifying the format
    try:
        return pd.to_datetime(date_str, format='%Y%B%d')
    except ValueError:
        return pd.NaT  # Return NaT if parsing fails

df_drop['Expiry'] = df_drop['Expiry'].apply(convert_to_timestamp)

grouped_df = df_output.groupby(['Symbol', 'Expiry', 'Option_Type'])['Strike'].unique().reset_index()
flag = True
for index, row in grouped_df.iterrows():
    print(index)
    # print(type(row))
    symbol = row['Symbol']
    expiry = row['Expiry']
    opttype = row['Option_Type']
    for each_strike in row['Strike']:
        # print(symbol, expiry, opttype, each_strike)
        temp_op_df = df_output.query("Symbol == @symbol and Expiry == @expiry and Option_Type == @opttype and Strike == @each_strike")
        temp_drop_df = df_drop.query("Symbol == @symbol and Expiry == @expiry and InstType == @opttype and StrikePrice == @each_strike")
        # print(temp_op_df)

        op_buy_qty = sum(temp_op_df['BuyQty'])
        drop_buy_qty = sum(temp_drop_df['BuyQty'])
        temp_op_df.loc[:, 'buy_value'] = temp_op_df['BuyQty'] * temp_op_df['BuyPrice']
        op_buy_value = temp_op_df['buy_value'].sum()
        temp_drop_df.loc[:, 'buy_value'] = temp_drop_df['BuyQty'] * temp_drop_df['BuyPrice']
        drop_buy_value = temp_drop_df['buy_value'].sum()

        op_sell_qty = sum(temp_op_df['SellQty'])
        drop_sell_qty = sum(temp_drop_df['SellQty'])
        temp_op_df.loc[:, 'sell_value'] = temp_op_df['SellQty'] * temp_op_df['SellPrice']
        op_sell_value = temp_op_df['sell_value'].sum()
        temp_drop_df.loc[:, 'sell_value'] = temp_drop_df['SellQty'] * temp_drop_df['SellPrice']
        drop_sell_value = temp_drop_df['sell_value'].sum()

        if op_buy_qty == drop_buy_qty:
            if op_buy_value == drop_buy_value:
                continue
            else:
                print(f'Buy Value Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}')
                flag = False
        else:
            print(f'Buy Quantity Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}')
            flag = False

        if op_sell_qty == drop_sell_qty:
            if op_sell_value == drop_sell_value:
                continue
            else:
                print(f'Sell Value Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}')
                flag = False
        else:
            print(f'Sell Quantity Mismatch: {symbol}, {expiry}, {opttype}, {each_strike}')
            flag = False
if flag:
    print('No mismatch between the dropcopy and inhouse consolidated trade file')