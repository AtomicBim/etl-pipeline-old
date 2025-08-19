from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

user = 'cdruser'
password = 'user12cdr'
host = '192.168.33.230'
database = 'asteriskcdrdb'

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4"
)

csv_path = OUT_DIR / 'asterisk_export_cdr.csv'

# Маска номеров
people_dict = {
    "Галиева Елена Рашидовна": "7447",
    "Попов Антон Владимирович": "7572",
    "Красильников Дмитрий Сергеевич": "7444",
    "Коновалов Василий Сергеевич": "7832",
    "Пятков Роман Анатольевич": "89022665789",
    "Колпаков Семен Дмитриевич": "7447",
    "Васьков Денис Игоревич": "7456",
    "Овсянкин Роман Николаевич": "7440",
    "Панов Антон Владимирович": "7441",
    "Григорьев Роман Николаевич": "7450",
    "Литуева Юлия Дмитриевна": "7835"
}

# Извлекаем только значения (номера) из словаря
allowed_dst_values = set(people_dict.values())


# 1. Получаем максимум uniqueid из текущего CSV (или calldate)
if csv_path.exists():
    # Для экономии памяти читаем только нужный столбец
    try:
        last_ids = pd.read_csv(csv_path, usecols=['uniqueid'])
        last_max = last_ids['uniqueid'].max()
        del last_ids
    except Exception:
        last_max = None
else:
    last_max = None

# 2. Строим запрос для выборки только новых строк
if last_max is not None and pd.notna(last_max):
    query = f"SELECT * FROM cdr WHERE uniqueid > '{last_max}'"
else:
    query = "SELECT * FROM cdr"

# 3. Получаем только новые строки
df_new = pd.read_sql(query, engine)

# 4. Очищаем и фильтруем по dst
df_new_clean = df_new.dropna(axis=0, how='all').dropna(axis=1, how='all')

# Фильтрация: только если dst содержится в маске
df_new_clean = df_new_clean[df_new_clean['dst'].astype(str).isin(allowed_dst_values)]

# 5. Добавляем только новые строки к CSV (append)
if not df_new_clean.empty:
    df_new_clean.to_csv(csv_path, mode='a', index=False, header=not csv_path.exists(), encoding='utf-8')
    print(f"Добавлено {len(df_new_clean)} новых строк в файл: {csv_path}")
else:
    print("Новых строк для добавления нет.")