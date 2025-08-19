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
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

SERVICE_ACCOUNT_FILE = BASE_DIR / 'config' / 'revitmaterials-4c3f80dae9f5.json' 
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
ws = (
    gspread.authorize(creds)
    .open_by_key('1GFxOABxNsPxWWQ9XzeERu90CB850Fp5j9NnH8zD01eQ')
    .worksheet('Навыки пользователя')
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

cleaned = []
for r in data_rows:
    # выравниваем длину под число колонок шапки
    r = r + [''] * (len(header) - len(r))
    # переводим CR/LF → пробел
    r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]
    # «Описание изменений» → только первая строка
    if desc_col is not None and r[desc_col]:
        r[desc_col] = r[desc_col].split(' ')[0]
    cleaned.append(r)

# ------------------------------------------------------------
# Сохраняем с оригинальной шапкой
# ------------------------------------------------------------
df = pd.DataFrame(cleaned, columns=header)
df.to_csv(
    OUT_DIR / 'gsheet_export_instructions.csv',
    index=False,
    header=True,                # <-- выводим заголовки
    quoting=csv.QUOTE_ALL,
    encoding='utf-8'
)