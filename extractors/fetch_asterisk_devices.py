from sqlalchemy import create_engine
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

user = 'cdruser'
password = 'user12cdr'
host = '192.168.33.230'
database = 'asterisk'

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4"
)

csv_path = OUT_DIR / 'asterisk_export_devices.csv'

try:
    # Читаем таблицу в pandas через SQLAlchemy
    df = pd.read_sql("SELECT * FROM devices;", engine)

    # Удаляем полностью пустые строки и столбцы
    df_clean = df.dropna(axis=0, how='all').dropna(axis=1, how='all')

    # Жёстко исключаем столбец 'emergency_cid' (даже если он случайно вернётся)
    df_clean = df_clean.loc[:, df_clean.columns != 'emergency_cid']

    # Сохраняем в CSV
    df_clean.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"Сохранено {len(df_clean)} строк в файл: {csv_path}")

except Exception as err:
    print(f"Ошибка: {err}")