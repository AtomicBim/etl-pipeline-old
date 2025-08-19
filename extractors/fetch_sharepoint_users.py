import requests
from requests_ntlm import HttpNtlmAuth
import urllib3
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

OUTPUT_FILE = 'sharepoint_export_users.csv'

# Параметры подключения к SharePoint
USERNAME = 'DOM\\r.grigoriev'
PASSWORD = 'Salgado123jnfdby'
SHAREPOINT_URL = "https://life.atomsk.ru/BIM/_api/web/siteusers"
REQUIRED_COLUMNS = ['Id', 'Title', 'Email', 'LoginName']

# Функции

def setup_requests_session():
    """Настраивает сессию requests с отключением SSL предупреждений."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    headers = {
        "Accept": "application/json;odata=verbose"
    }
    
    return headers

def fetch_sharepoint_users():
    """Получает пользователей из SharePoint."""
    headers = setup_requests_session()
    
    response = requests.get(
        SHAREPOINT_URL,
        auth=HttpNtlmAuth(USERNAME, PASSWORD),
        headers=headers,
        verify=False
    )
    
    if response.status_code == 200:
        data = response.json()
        users = data['d']['results']
        logger.info(f"Получено пользователей из SharePoint: {len(users)}")
        return users
    else:
        logger.error(f"Ошибка при получении пользователей: {response.status_code}")
        logger.error(f"Ответ сервера: {response.text}")
        raise Exception(f"Ошибка HTTP {response.status_code}: {response.text}")

def process_users_data(users):
    """Обрабатывает данные пользователей и фильтрует нужные поля."""
    df_users = pd.DataFrame(users)
    
    # Проверяем наличие необходимых столбцов
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df_users.columns]
    if missing_columns:
        logger.warning(f"Отсутствуют столбцы: {missing_columns}")
    
    # Выбираем только доступные столбцы из требуемых
    available_columns = [col for col in REQUIRED_COLUMNS if col in df_users.columns]
    df_users = df_users[available_columns]
    
    logger.info(f"Обработано пользователей: {len(df_users)} с полями: {available_columns}")
    return df_users

def main():
    """Основная функция для извлечения пользователей из SharePoint."""
    try:
        logger.info("Начало извлечения пользователей из SharePoint")
        
        save_path = OUT_DIR / OUTPUT_FILE
        
        # Получаем данные пользователей из SharePoint
        logger.info("Подключение к SharePoint и получение пользователей...")
        users = fetch_sharepoint_users()
        
        # Обрабатываем данные
        logger.info("Обработка данных пользователей...")
        df_users = process_users_data(users)
        
        # Сохраняем в CSV
        logger.info("Сохранение данных в CSV...")
        df_users.to_csv(save_path, index=False, encoding='utf-8')
        
        logger.info(f"Данные пользователей успешно сохранены в файл: {save_path}")
        logger.info(f"Извлечение пользователей из SharePoint завершено успешно. Обработано пользователей: {len(df_users)}")
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении пользователей из SharePoint: {e}")
        raise

if __name__ == "__main__":
    main()