#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
Выгружает GUID-справочники из Yougile и сохраняет их на Яндекс.Диск.

Каталог назначения:
    C:\Users\Roman\YandexDisk\Обмен

Создаются файлы:
    ├─ yougile_users.json     – [{ id, name }]
    ├─ yougile_projects.json  – [{ id, title }]
    └─ yougile_stickers.json  – [{ stickerId, stateId, stateName }]
"""

import json
import time
import requests
from pathlib import Path
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

logger = setup_logging(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

# ── конфигурация ───────────────────────────────────────────────────────────
TOKEN = "Mn0MxS4Sp+BRTXmZG3G3Q3R1+IHVPmXMbvZgrq51QaLCc9h5NxmhJOh0WqUZxqHH"
BASE = "https://yougile.com/api-v2"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
PAGE_LIMIT = 1000

# ── утилиты ────────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update(HEADERS)

def api_get(ep: str, params=None, retries: int = 4) -> dict:
    url = f"{BASE}/{ep}"
    for n in range(retries):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:                        # Too Many Requests
            time.sleep(int(resp.headers.get("Retry-After", 2 ** n)))
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Не удалось получить {url}")

def paged(ep: str, key: str):
    offset = 0
    while True:
        data = api_get(ep, {"limit": PAGE_LIMIT, "offset": offset})
        items = data.get(key) or data.get("content") or []
        yield from items
        if not data.get("paging", {}).get("next") and len(items) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

def main():
    logger.info("Запуск экспорта GUID-справочников из Yougile")
    
    try:
        # Пользователи
        logger.info("Загрузка пользователей...")
        users = [{"id": u["id"], "name": u["realName"]}
                 for u in paged("users", "content")]
        logger.info(f"Получено пользователей: {len(users)}")
        
        # Проекты
        logger.info("Загрузка проектов...")
        projects = [{"id": p["id"], "title": p["title"]}
                    for p in paged("projects", "projects")]
        logger.info(f"Получено проектов: {len(projects)}")
        
        # Строковые стикеры
        logger.info("Загрузка стикеров...")
        stickers = [
            {"stickerId": s["id"], "stateId": st["id"], "stateName": st["name"]}
            for s in paged("string-stickers", "stickers")
            for st in s.get("states", [])
        ]
        logger.info(f"Получено стикеров: {len(stickers)}")
        
        # Сохранение
        logger.info("Сохранение файлов...")
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        
        files_saved = []
        
        with open(OUT_DIR / "yougile_users.json", "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
            files_saved.append("yougile_users.json")
        
        with open(OUT_DIR / "yougile_projects.json", "w", encoding="utf-8") as f:
            json.dump(projects, f, ensure_ascii=False, indent=2)
            files_saved.append("yougile_projects.json")
        
        with open(OUT_DIR / "yougile_stickers.json", "w", encoding="utf-8") as f:
            json.dump(stickers, f, ensure_ascii=False, indent=2)
            files_saved.append("yougile_stickers.json")
        
        logger.info(f"Экспорт завершен успешно. Файлы сохранены в: {OUT_DIR}")
        logger.info(f"Созданы файлы: {', '.join(files_saved)}")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте GUID-справочников: {e}")
        raise

if __name__ == "__main__":
    main()