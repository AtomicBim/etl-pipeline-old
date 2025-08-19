import psycopg2
from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

# Константы и конфигурация
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = 'tim_export_plugin.csv'

# Параметры подключения к PostgreSQL
CONN_PARAMS = {
    "host": "192.168.42.188",
    "port": 5430,
    "dbname": "pluginsdb",
    "user": "postgres",
    "password": "Q!w2e3r4"
}

SCHEMA = "plugins"
TABLE = "plugin"

# Функции

def export_table_to_csv(output_path):
    """Экспортирует таблицу PostgreSQL в CSV файл."""
    copy_sql = f'COPY "{SCHEMA}"."{TABLE}" TO STDOUT WITH CSV HEADER ENCODING \'UTF8\''
    
    with psycopg2.connect(**CONN_PARAMS) as conn, open(output_path, "w", encoding="utf-8") as f:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, f)

def main():
    """Основная функция для экспорта таблицы plugin из PostgreSQL."""
    try:
        logger.info("Начало экспорта таблицы plugin из PostgreSQL")
        
        output_path = OUT_DIR / OUTPUT_FILE
        
        # Подключение и экспорт
        logger.info(f"Подключение к PostgreSQL и экспорт таблицы {SCHEMA}.{TABLE}...")
        export_table_to_csv(output_path)
        
        logger.info(f"Таблица {SCHEMA}.{TABLE} успешно экспортирована в файл: {output_path}")
        logger.info("Экспорт данных plugin завершен успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте таблицы plugin: {e}")
        raise

if __name__ == "__main__":
    main()