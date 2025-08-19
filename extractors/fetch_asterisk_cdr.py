from sqlalchemy import create_engine
import pandas as pd
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

# Параметры подключения к базе данных
DB_USER = 'cdruser'
DB_PASSWORD = 'user12cdr'
DB_HOST = '192.168.33.230'
DB_NAME = 'asteriskcdrdb'
OUTPUT_FILE = 'asterisk_export_cdr.csv'

# Маска номеров
PEOPLE_DICT = {
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

# Функции

def create_db_engine():
    """Создает подключение к базе данных MySQL."""
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"
    )

def get_last_uniqueid(csv_path):
    """Получает максимальный uniqueid из существующего CSV файла."""
    if csv_path.exists():
        try:
            last_ids = pd.read_csv(csv_path, usecols=['uniqueid'])
            last_max = last_ids['uniqueid'].max()
            del last_ids
            return last_max
        except Exception as e:
            logger.warning(f"Не удалось прочитать последний uniqueid: {e}")
            return None
    return None

def build_query(last_max):
    """Строит SQL запрос для выборки данных."""
    if last_max is not None and pd.notna(last_max):
        query = f"SELECT * FROM cdr WHERE uniqueid > '{last_max}'"
        logger.info(f"Запрос для новых записей с uniqueid > {last_max}")
    else:
        query = "SELECT * FROM cdr"
        logger.info("Запрос для всех записей (первичная загрузка)")
    return query

def filter_data(df):
    """Очищает и фильтрует данные по маске номеров."""
    # Извлекаем только значения (номера) из словаря
    allowed_dst_values = set(PEOPLE_DICT.values())
    
    # Очищаем данные
    df_clean = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
    
    # Фильтрация: только если dst содержится в маске
    df_filtered = df_clean[df_clean['dst'].astype(str).isin(allowed_dst_values)]
    
    logger.info(f"Отфильтровано записей: {len(df_filtered)} из {len(df_clean)}")
    return df_filtered

def save_to_csv(df, csv_path, is_append=True):
    """Сохраняет данные в CSV файл."""
    if not df.empty:
        header = not csv_path.exists() if is_append else True
        mode = 'a' if is_append else 'w'
        
        df.to_csv(csv_path, mode=mode, index=False, header=header, encoding='utf-8')
        logger.info(f"Добавлено {len(df)} новых строк в файл: {csv_path}")
        return len(df)
    else:
        logger.info("Новых строк для добавления нет")
        return 0

def main():
    """Основная функция для извлечения данных CDR из Asterisk."""
    try:
        logger.info("Начало извлечения данных CDR из Asterisk")
        
        csv_path = OUT_DIR / OUTPUT_FILE
        
        # Создаем подключение к БД
        logger.info("Подключение к базе данных...")
        engine = create_db_engine()
        
        # Получаем максимальный uniqueid из существующего файла
        logger.info("Определение последней обработанной записи...")
        last_max = get_last_uniqueid(csv_path)
        
        # Строим запрос для выборки только новых строк
        query = build_query(last_max)
        
        # Получаем данные из БД
        logger.info("Выполнение запроса к базе данных...")
        df_new = pd.read_sql(query, engine)
        logger.info(f"Получено записей из БД: {len(df_new)}")
        
        # Очищаем и фильтруем данные
        logger.info("Фильтрация данных по маске номеров...")
        df_filtered = filter_data(df_new)
        
        # Сохраняем данные
        logger.info("Сохранение данных в CSV...")
        saved_count = save_to_csv(df_filtered, csv_path)
        
        logger.info(f"Извлечение данных CDR завершено успешно. Обработано записей: {saved_count}")
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных CDR: {e}")
        raise

if __name__ == "__main__":
    main()