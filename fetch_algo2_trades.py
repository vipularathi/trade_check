import pandas as pd
import requests, json, os
from datetime import datetime

root_dir = os.getcwd()
their_file_path = os.path.join(root_dir, 'Their_file')

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


def get_algo2_trades():
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


if __name__ == "__main__":
    algo2_df = get_algo2_trades()
    if not algo2_df.empty:
        algo2_df.to_excel(os.path.join(their_file_path, rf'dropcopy_algo2_positions_'
                                                    rf'{datetime.today().date().strftime("%Y%m%d")}_165710.xlsx'))
        print('Algo2 trades fetched.')
    else:
        print('No Algo2 trades.')