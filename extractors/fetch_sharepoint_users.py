import requests
from requests_ntlm import HttpNtlmAuth
import urllib3
import pandas as pd
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)
save_path = OUT_DIR / 'sharepoint_export_users.csv'

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

username = 'DOM\\r.grigoriev'
password = 'Salgado123jnfdby'

url = "https://life.atomsk.ru/BIM/_api/web/siteusers"
headers = {
    "Accept": "application/json;odata=verbose"
}

response = requests.get(
    url,
    auth=HttpNtlmAuth(username, password),
    headers=headers,
    verify=False
)

if response.status_code == 200:
    data = response.json()
    users = data['d']['results']

    df_users = pd.DataFrame(users)
    df_users = df_users[['Id', 'Title', 'Email', 'LoginName']]

    df_users.to_csv(save_path, index=False, encoding='utf-8')

    print(f"Сохранено {len(df_users)} пользователей в '{save_path}'")
else:
    print("Ошибка при получении пользователей:")
    print(response.text)