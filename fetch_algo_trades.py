import pandas as pd
import requests, json, os
from datetime import datetime
import numpy as np

root_dir = os.getcwd()
their_file_path = os.path.join(root_dir, 'Their_file')
our_file_path = os.path.join(root_dir,'Our_file')
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


def get_algo2_trades():
    server_ip = "192.168.50.68"
    adminusername = "user3_rms"
    adminpassword = "user3_rms"
    userid = 201
    
    url = f"http://{server_ip}:8010/v1/loginrms"
    payload = json.dumps({
        "username": adminusername,
        "password": adminpassword
    })
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
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
    response = requests.request("GET", url, headers=headers, data=payload)
    
    if response.status_code == 200:
        response = response.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame()

def get_algo3_trades():
    url = f'http://172.16.47.56:8035/net_position_calc'
    payload = json.dumps({
        "pagination": {
            "current": 1,
            "pageSize": 1000
        },
        "sorter": {},
        "filters": {}
    })
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'http://172.16.47.230:5177',
        'Referer': 'http://172.16.47.230:5177/',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36'
    }
    resp = requests.post(url=url,headers=headers, data=payload)
    if resp.status_code == 200:
        response = resp.json()
        data = response['data']
        df = pd.DataFrame(data)
        return df

if __name__ == "__main__":
    algo2_df = get_algo2_trades()
    if not algo2_df.empty:
        algo2_df.rename(columns={'strikePrice':'StrikePrice','sellQty':'SellQty'}, inplace=True)
        algo2_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo2_positions_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'))
        algo2_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo2_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}.csv'))
        print('Algo2 trades fetched.')
    else:
        print('No Algo2 trades.')
    algo3_df = get_algo3_trades()
    if not algo3_df.empty:
        algo3_df.rename(columns={'Ser_Exp': 'Expiry', 'OptionType': 'InstType', 'BuyAvg': 'BuyPrice', 'SellAvg': 'SellPrice'},
                  inplace=True)
        algo3_df.NetQty = algo3_df.NetQty.astype(np.int64)
        col_keep = ['Symbol', 'Expiry', 'StrikePrice', 'InstType', 'BuyQty', 'BuyPrice', 'SellQty', 'SellPrice',
                    'NetQty']
        algo3_df.drop(columns=[col for col in algo3_df.columns if col not in col_keep], inplace=True)
        algo3_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo3_pos_dc_positions_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'))
        algo3_df.to_csv(os.path.join(our_file_path, rf'net_positions_algo3_pos_dc_'
                                                        rf'{datetime.today().date().strftime("%Y%m%d")}.csv'))
        print('Algo3 trades fetched.')
    else:
        print('No Algo3 trades.')