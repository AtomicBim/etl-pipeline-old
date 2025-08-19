import requests
import json
import sys
import os
from pathlib import Path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

# Load token from config
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / 'config' / 'tokens.json'
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    config = json.load(f)
TOKEN = config['yandex']['token']

def main():
    logger.info("Запуск проверки токена Yandex API")
    
    headers = {
        'Authorization': f'OAuth {TOKEN}'
    }
    
    url = 'https://api-metrika.yandex.net/management/v1/counters'
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Проверка токена Yandex API завершена успешно. Статус: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к Yandex API: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        raise

if __name__ == "__main__":
    main()