import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import csv
from pathlib import Path

# ------------------------------------------------------------
# Авторизация
# ------------------------------------------------------------
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'config'
OUT_DIR.mkdir(exist_ok=True)

SERVICE_ACCOUNT_FILE = BASE_DIR / 'config' / 'revitmaterials-4c3f80dae9f5.json' 
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
ws = (
    gspread.authorize(creds)
    .open_by_key('19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck')
    .worksheet('gitlab-plugins')
)

# ------------------------------------------------------------
# Читаем всё: первая строка = заголовки, остальные = данные
# ------------------------------------------------------------
values      = ws.get_all_values()               # всё как есть
header      = values[0]                         # оригинальные заголовки
data_rows   = values[1:]                        # только данные

# ------------------------------------------------------------
# Подчищаем данные (заголовки не трогаем!)
# ------------------------------------------------------------
try:
    desc_col = header.index('Описание изменений')
except ValueError:
    desc_col = None

# ------------------------------------------------------------
# Подчищаем данные (только выравнивание строк и переносы)
# ------------------------------------------------------------
cleaned = []
for r in data_rows:
    r = r + [''] * (len(header) - len(r))  # выравниваем длину под заголовки
    r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]  # убираем переносы
    cleaned.append(r)

# ------------------------------------------------------------
# Сохраняем с оригинальной шапкой
# ------------------------------------------------------------
df = pd.DataFrame(cleaned, columns=header)
df.to_csv(
    OUT_DIR / 'gitlab-plugins_mapping.csv',
    index=False,
    header=True,                # <-- выводим заголовки
    encoding='utf-8'
)