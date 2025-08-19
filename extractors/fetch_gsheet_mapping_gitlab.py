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

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'config'
OUT_DIR.mkdir(exist_ok=True)

def main():
    logger.info("Запуск экспорта маппинга GitLab из Google Sheets")
    
    SERVICE_ACCOUNT_FILE = BASE_DIR / 'config' / 'revitmaterials-4c3f80dae9f5.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        ws = (
            gspread.authorize(creds)
            .open_by_key('19ZDWnS0Ft8bLVCbVyHsOatTTzidv55r5Rj7Woi9mNck')
            .worksheet('gitlab-plugins')
        )
        
        logger.info("Загрузка данных из таблицы...")
        values = ws.get_all_values()
        header = values[0]
        data_rows = values[1:]
        
        logger.info(f"Получено строк данных: {len(data_rows)}")
        
        # Подчищаем данные
        try:
            desc_col = header.index('Описание изменений')
        except ValueError:
            desc_col = None
        
        cleaned = []
        for r in data_rows:
            r = r + [''] * (len(header) - len(r))
            r = [c.replace('\r', ' ').replace('\n', ' ') for c in r]
            cleaned.append(r)
        
        # Сохраняем данные
        df = pd.DataFrame(cleaned, columns=header)
        output_file = OUT_DIR / 'gitlab-plugins_mapping.csv'
        df.to_csv(
            output_file,
            index=False,
            header=True,
            encoding='utf-8'
        )
        
        logger.info(f"Экспорт завершен успешно. Файл: {output_file}")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте маппинга GitLab: {e}")
        raise

if __name__ == "__main__":
    main()