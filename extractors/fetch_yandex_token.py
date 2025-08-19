import requests

TOKEN = 'y0__xDlneKOAxjG8zgggsKE5ROQ8kQL_hrsA9BJs5PDa4vwBdL_hg'

headers = {
    'Authorization': f'OAuth {TOKEN}'
}

url = 'https://api-metrika.yandex.net/management/v1/counters'

response = requests.get(url, headers=headers)
# print(response.text)