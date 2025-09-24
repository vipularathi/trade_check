import pandas as pd
import requests, json, os
from datetime import datetime, date
import numpy as np
from io import StringIO

root_dir = os.getcwd()
qi_dir = os.path.join(root_dir,'QI_files')
their_file_path = os.path.join(root_dir, 'Their_file')
our_file_path = os.path.join(root_dir,'Our_file')
proxies = {'http':None,'https':None}
net_pos_url = 'http://172.16.47.87:5000/download/'
# server_ip = "192.168.50.68"
# adminusername = "user3_rms"
# adminpassword = "user3_rms"
# userid = 201
#
# url = f"http://{server_ip}:8010/v1/loginrms"
# payload = json.dumps({
#     "username": adminusername,
#     "password": adminpassword
# })
# headers = {
#     'accept': 'application/json',
#     'Content-Type': 'application/json'
# }
#
# response = requests.request("POST", url, headers=headers, data=payload)
# response_msg = response.json()
# authtoken = response_msg.get("token")

format_dict = {
        'Symbol': str,
        'StrikePrice': np.int64,
        'InstType': str,
        'BuyQty': np.int64,
        'BuyPrice': np.float64,
        'SellQty': np.int64,
        'SellPrice': np.float64,
        'NetQty': np.int64
    }

# def convert_to_timestamp(date_input):
#     if isinstance(date_input, date):
#         return date_input
#     if isinstance(date_input, str):
#         date_str = date_input.replace('st', '').replace('nd', '').replace('rd', '').replace('th', '').replace(' ','')
#         date_format = ['%Y%B%d', '%Y-%m-%d', '%d-%m-%Y', '%d%b%Y']
#         for each in date_format:
#             try:
#                 # return pd.to_datetime(date_str, format=each).date()
#                 return datetime.strptime(date_str, each).date()
#             except ValueError:
#                 continue
#     return pd.NaT

def get_file_downloader_trade(for_server, columns_present):
    resp = requests.get(url=rf"{net_pos_url + for_server}", proxies=proxies)
    # df = pd.DataFrame()
    if resp.status_code != 200:
        print(f'No trade found in file downloader for {for_server}')
        return pd.DataFrame(columns=columns_present)
    df = pd.read_csv(StringIO(resp.text))
    # df['Expiry'] = df['Expiry'].apply(convert_to_timestamp)
    return df

def get_algo2_trades():
    server_ip = "192.168.50.68"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    proxies = {"http": None, "https": None}

    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies)
    response_msg = response.json()
    authtoken = response_msg.get("token")
    url = f"http://{server_ip}:8010/v1/dcNetposition?user_id={userid}"
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/137.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'auth-token': authtoken
    }
    response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
    
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

def get_algo3_trades():
    server_ip = "192.168.50.41"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    proxies = {"http": None, "https": None}
    
    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies)
    response_msg = response.json()
    authtoken = response_msg.get("token")
    url = f"http://{server_ip}:8010/v1/dcNetposition?user_id={userid}"
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/137.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'auth-token': authtoken
    }
    response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

# def get_algo3_trades():
#     proxies = {"http": None, "https": None}
#     url = f'http://172.16.47.56:8041/net_position_calc'
#     payload = json.dumps({
#         "pagination": {
#             "current": 1,
#             "pageSize": 1000
#         },
#         "sorter": {},
#         "filters": {}
#     })
#     headers = {
#         'Accept': 'application/json, text/plain, */*',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Connection': 'keep-alive',
#         'Content-Type': 'application/json',
#         'Origin': 'http://172.16.47.230:5177',
#         'Referer': 'http://172.16.47.230:5177/',
#         'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36'
#     }
#     resp = requests.post(url=url,headers=headers, data=payload, proxies=proxies)
#     if resp.status_code == 200:
#         response = resp.json()
#         data = response['data']
#         df = pd.DataFrame(data)
#         return df

def get_algo81_dc_trades():
    server_ip = "192.168.50.81"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    proxies = {"http": None, "https": None}
    
    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies)
    response_msg = response.json()
    authtoken = response_msg.get("token")
    url = f"http://{server_ip}:8010/v1/dcNetposition?user_id={userid}"
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/137.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'auth-token': authtoken
    }
    response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
    
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

def get_algo81_port_trades():
    server_ip = "192.168.50.81"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    proxies = {"http": None, "https": None}
    
    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies)
    response_msg = response.json()
    authtoken = response_msg.get("token")
    # url = f"http://{server_ip}:8010/v1/dcNetposition?user_id={userid}"
    url = f"http://{server_ip}:8010/v1/netposition?mode=f"
    payload = {}
    headers = {
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE',
        'Access-Control-Allow-Origin': '*',
        'Referer': 'http://192.168.50.81:5173/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Access-Control-Allow-Headers': '*',
        'auth-token': authtoken
    }
    
    response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
    
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

def get_algo82_trades():
    server_ip = "192.168.50.82"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    proxies = {"http": None, "https": None}
    
    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, proxies=proxies)
    response_msg = response.json()
    authtoken = response_msg.get("token")
    url = f"http://{server_ip}:8010/v1/dcNetposition?user_id={userid}"
    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/137.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'auth-token': authtoken
    }
    response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
    
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    algo2_df = get_algo2_trades()
    if not algo2_df.empty:
        algo2_df.rename(columns={'strikePrice':'StrikePrice','sellQty':'SellQty'}, inplace=True)
        algo2_df = algo2_df.astype(format_dict)
        # algo2_df['Expiry'] = algo2_df['Expiry'].apply(convert_to_timestamp)
        algo2_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo2_positions_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'),
                          index=False)
        algo2_net_pos_df = get_file_downloader_trade(for_server='algo2_pos', columns_present = algo2_df.columns.tolist())
        algo2_net_pos_df = algo2_net_pos_df.astype(format_dict)
        # algo2_net_pos_df['Expiry'] = algo2_net_pos_df['Expiry'].apply(convert_to_timestamp)
        algo2_net_pos_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo2_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}.csv'), index=False)
        print('Algo68 trades fetched.')
    else:
        print('No Algo68 trades.')
    algo3_df = get_algo3_trades()
    if not algo3_df.empty:
        algo3_df.rename(columns={'Ser_Exp': 'Expiry', 'OptionType': 'InstType', 'BuyAvg': 'BuyPrice', 'SellAvg': 'SellPrice'},
                  inplace=True)
        col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                    'NetQty']
        algo3_df.drop(columns=[col for col in algo3_df.columns if col not in col_keep], inplace=True)
        algo3_df = algo3_df.astype(format_dict)
        # algo3_df['Expiry'] = algo3_df['Expiry'].apply(convert_to_timestamp)
        algo3_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo41_pos_dc_positions_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'), index=False)
        algo3_net_pos_df = get_file_downloader_trade(for_server='algo41_pos_dc',
                                                     columns_present=algo3_df.columns.tolist())
        algo3_net_pos_df = algo3_net_pos_df.astype(format_dict)
        # algo3_net_pos_df['Expiry'] = algo3_net_pos_df['Expiry'].apply(convert_to_timestamp)
        algo3_net_pos_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo41_pos_dc_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}.csv'), index=False)
        print('Algo41 trades fetched.')
    else:
        print('No Algo41 trades.')
    algo81_df = get_algo81_port_trades()
    if not algo81_df.empty:
        col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                    'NetQty']
        algo81_df.drop(columns=[col for col in algo81_df if col not in col_keep],inplace=True)
        algo81_df = algo81_df.astype(format_dict)
        # algo81_df['Expiry'] = algo81_df['Expiry'].apply(convert_to_timestamp)
        algo81_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo81_pos_dc_positions_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'), index=False)
        algo81_net_pos_df = get_file_downloader_trade(for_server='algo81_pos_dc',
                                                      columns_present=algo81_df.columns.tolist())
        algo81_net_pos_df = algo81_net_pos_df.astype(format_dict)
        # algo81_net_pos_df['Expiry'] = algo81_net_pos_df['Expiry'].apply(convert_to_timestamp)
        algo81_net_pos_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo81_pos_dc_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}.csv'), index=False)
        print('Algo81 trades fetched.')
    else:
        print('No Algo81 trades.')
    algo82_df = get_algo82_trades()
    if not algo82_df.empty:
        col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                    'NetQty']
        algo82_df.drop(columns=[col for col in algo82_df if col not in col_keep],inplace=True)
        algo82_df = algo82_df.astype(format_dict)
        # algo82_df['Expiry'] = algo82_df['Expiry'].apply(convert_to_timestamp)
        algo82_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo82_pos_dc_positions_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'), index=False)
        algo82_net_pos_df = get_file_downloader_trade(for_server='algo82_pos_dc',
                                                      columns_present=algo82_df.columns.tolist())
        algo82_net_pos_df = algo82_net_pos_df.astype(format_dict)
        # algo82_net_pos_df['Expiry'] = algo82_net_pos_df['Expiry'].apply(convert_to_timestamp)
        algo82_net_pos_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo82_pos_dc_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}.csv'), index=False)
        print('Algo82 trades fetched.')
    else:
        print('No Algo82 trades.')
    p=0