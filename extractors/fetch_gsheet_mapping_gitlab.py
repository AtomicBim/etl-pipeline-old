import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import csv
import json
from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'config'
OUT_DIR.mkdir(exist_ok=True)
CONFIG_PATH = BASE_DIR / 'config' / 'tokens.json'

# Load spreadsheet key from config
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = json.load(f)
SPREADSHEET_KEY = config['google_sheets']['spreadsheet_keys'][0]['key']

def main():
    logger.info("Запуск экспорта маппинга GitLab из Google Sheets")
    
    SERVICE_ACCOUNT_FILE = BASE_DIR / 'config' / 'revitmaterials-e79d24766ccd.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        ws = (
            gspread.authorize(creds)
            .open_by_key(SPREADSHEET_KEY)
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