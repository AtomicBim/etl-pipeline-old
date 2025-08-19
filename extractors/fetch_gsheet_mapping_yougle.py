import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import csv
from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

# ------------------------------------------------------------
# Константы и конфигурация
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'config'
OUT_DIR.mkdir(exist_ok=True)

SERVICE_ACCOUNT_FILE = BASE_DIR / 'config' / 'revitmaterials-e79d24766ccd.json' 
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_KEY = '19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck'
WORKSHEET_NAME = 'yougile-plugins'
OUTPUT_FILE = 'yougile-plugins_mapping.csv'

# ------------------------------------------------------------
# Функции
# ------------------------------------------------------------

def get_worksheet():
    """Получает рабочий лист из Google Sheets."""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    ws = (
        gspread.authorize(creds)
        .open_by_key(SPREADSHEET_KEY)
        .worksheet(WORKSHEET_NAME)
    )
    return ws

def clean_data(data_rows, header):
    """Очищает данные - выравнивает строки и убирает переносы."""
    cleaned = []
    for r in data_rows:
        r = r + [''] * (len(header) - len(r))  # выравниваем длину под заголовки
        r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]  # убираем переносы
        cleaned.append(r)
    return cleaned

def main():
    """Основная функция для извлечения маппинга yougile-plugins из Google Sheets."""
    try:
        logger.info("Начало извлечения данных из Google Sheets (yougile-plugins)")
        
        # Авторизация и получение листа
        logger.info("Подключение к Google Sheets...")
        ws = get_worksheet()
        
        # Читаем всё: первая строка = заголовки, остальные = данные
        logger.info("Чтение данных из листа...")
        values = ws.get_all_values()
        header = values[0]
        data_rows = values[1:]
        logger.info(f"Получено строк данных: {len(data_rows)}")
        
        # Подчищаем данные (заголовки не трогаем!)
        try:
            desc_col = header.index('Описание изменений')
            logger.debug(f"Найден столбец 'Описание изменений' с индексом: {desc_col}")
        except ValueError:
            desc_col = None
            logger.debug("Столбец 'Описание изменений' не найден")
        
        # Подчищаем данные (только выравнивание строк и переносы)
        logger.info("Очистка данных...")
        cleaned = clean_data(data_rows, header)
        
        # Сохраняем с оригинальной шапкой
        logger.info("Сохранение данных в CSV...")
        df = pd.DataFrame(cleaned, columns=header)
        output_path = OUT_DIR / OUTPUT_FILE
        df.to_csv(
            output_path,
            index=False,
            header=True,
            encoding='utf-8'
        )
        
        logger.info(f"Данные успешно сохранены в файл: {output_path}")
        logger.info(f"Обработано записей: {len(cleaned)}")
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных yougile-plugins: {e}")
        raise

if __name__ == "__main__":
    main()