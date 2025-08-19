from sqlalchemy import create_engine
from pathlib import Path
import pandas as pd
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

# Константы и конфигурация
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)
CONFIG_PATH = BASE_DIR / 'config' / 'tokens.json'

# Load credentials from config
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = json.load(f)

# Параметры подключения к базе данных
DB_USER = 'cdruser'
DB_PASSWORD = config['asterisk_db']['password']
DB_HOST = '192.168.33.230'
DB_NAME = 'asterisk'
OUTPUT_FILE = 'asterisk_export_devices.csv'

# Функции

def create_db_engine():
    """Создает подключение к базе данных MySQL."""
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
    )

def clean_data(df):
    """Очищает данные - удаляет пустые строки и столбцы, исключает emergency_cid."""
    # Удаляем полностью пустые строки и столбцы
    df_clean = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
    
    # Жёстко исключаем столбец 'emergency_cid' (даже если он случайно вернётся)
    df_clean = df_clean.loc[:, df_clean.columns != 'emergency_cid']
    
    logger.info(f"Очищено данных: {len(df_clean)} строк, {len(df_clean.columns)} столбцов")
    return df_clean

def main():
    """Основная функция для извлечения данных устройств из Asterisk."""
    try:
        logger.info("Начало извлечения данных устройств из Asterisk")
        
        csv_path = OUT_DIR / OUTPUT_FILE
        
        # Создаем подключение к БД
        logger.info("Подключение к базе данных...")
        engine = create_db_engine()
        
        # Читаем таблицу в pandas через SQLAlchemy
        logger.info("Выполнение запроса к таблице devices...")
        df = pd.read_sql("SELECT * FROM devices;", engine)
        logger.info(f"Получено записей из БД: {len(df)}")
        
        # Очищаем данные
        logger.info("Очистка данных...")
        df_clean = clean_data(df)
        
        # Сохраняем в CSV
        logger.info("Сохранение данных в CSV...")
        df_clean.to_csv(csv_path, index=False, encoding='utf-8')
        
        logger.info(f"Данные устройств успешно сохранены в файл: {csv_path}")
        logger.info(f"Извлечение данных устройств завершено успешно. Обработано записей: {len(df_clean)}")

    except Exception as err:
        logger.error(f"Ошибка при извлечении данных устройств: {err}")
        raise

if __name__ == "__main__":
    main()