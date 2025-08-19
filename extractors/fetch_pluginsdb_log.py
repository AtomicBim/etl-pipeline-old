import psycopg2
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

output_path = OUT_DIR / 'tim_export_log.csv'

# Параметры подключения
conn_params = {
    "host": "192.168.42.188",
    "port": 5430,
    "dbname": "pluginsdb",
    "user": "postgres",
    "password": "Q!w2e3r4"
}

schema = "plugins"
table = "log"

# Сформировать SQL-запрос COPY
copy_sql = f'COPY "{schema}"."{table}" TO STDOUT WITH CSV HEADER ENCODING \'UTF8\''

# Сохранение
with psycopg2.connect(**conn_params) as conn, open(output_path, "w", encoding="utf-8") as f:
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, f)

print(f"Готово. Таблица {schema}.{table} экспортирована в '{output_path}'")