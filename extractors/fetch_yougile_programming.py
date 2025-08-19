#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import time
import datetime as dt
from typing import Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

# ──────────── конфигурация ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_PATH = OUT_DIR / 'yougile_export_programming.json'

TOKEN = "Mn0MxS4Sp+BRTXmZG3G3Q3R1+IHVPmXMbvZgrq51QaLCc9h5NxmhJOh0WqUZxqHH"

BASE = "https://yougile.com/api-v2"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

TARGET_PROJECT = "Разработка"
PAGE_LIMIT = 1000

logger = setup_logging(__name__)

# ID стикера статуса
STATUS_STICKER_ID = "86376c27-0d3c-42c8-850a-095fec0006ed"

# Маппинг состояний стикера статуса
STATUS_STATES = {
    "5ddc1ee55f62": "У пользователей",
    "02ab2db20998": "В разработке",
    "a755f41c689d": "В перспективе",
    "00603206008c": "Устаревший",
    "f00f0c43e3dd": "Доработка",
    "adb006363cde": "У координаторов",
    "e7fc4f24c7a5": "У тестовой группы"
}

# ──────────── сессия HTTP с повторениями ────────────────────────────────
session = requests.Session()
session.headers.update(HEADERS)
retry_cfg = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods={"GET"},
)
session.mount("https://", HTTPAdapter(max_retries=retry_cfg))

# ──────────── вспомогательные функции ───────────────────────────────────

def extract_items(response: dict, primary_key: Optional[str] = None) -> List[dict]:
    """Извлекает список элементов из ответа API."""
    if primary_key:
        return response.get(primary_key) or response.get("content") or []
    return response.get("content") or []

def get_task_full_status(task: dict, debug: bool = False) -> Optional[str]:
    """Извлекает полный статус задачи из стикера статуса."""
    stickers = task.get("stickers", [])
    
    # Преобразуем dict в list, если необходимо
    if isinstance(stickers, dict):
        # Если stickers - это словарь, пробуем разные варианты
        if STATUS_STICKER_ID in stickers:
            # Если ключ - это ID стикера
            sticker_data = stickers[STATUS_STICKER_ID]
            if isinstance(sticker_data, str) and sticker_data in STATUS_STATES:
                return STATUS_STATES[sticker_data]
            elif isinstance(sticker_data, dict):
                state_id = sticker_data.get("stateId") or sticker_data.get("state")
                if state_id in STATUS_STATES:
                    return STATUS_STATES[state_id]
        
        # Преобразуем словарь в список для дальнейшей обработки
        stickers_list = []
        for key, value in stickers.items():
            if isinstance(value, dict):
                value["stickerId"] = key
                stickers_list.append(value)
            else:
                stickers_list.append({"stickerId": key, "stateId": value})
        stickers = stickers_list
    
    if debug:
        logger.debug(f"\n{'='*60}")
        logger.debug(f"Debug stickers for task: {task.get('title', 'Unknown')}")
        logger.debug(f"Task ID: {task.get('id', 'unknown')}")
        logger.debug(f"Original stickers type: {type(task.get('stickers'))}")
        logger.debug(f"Processed stickers type: {type(stickers)}")
        logger.debug(f"Stickers count: {len(stickers) if isinstance(stickers, list) else 'N/A'}")
        
        if isinstance(stickers, list):
            for i, sticker in enumerate(stickers):
                logger.debug(f"\nSticker {i+1}:")
                logger.debug(f"  Type: {type(sticker)}")
                if isinstance(sticker, dict):
                    logger.debug(f"  Keys: {list(sticker.keys())}")
                    logger.debug(f"  Content: {sticker}")
                else:
                    logger.debug(f"  Value: {sticker}")
        
        logger.debug(f"\nOriginal stickers data:")
        logger.debug(f"{task.get('stickers')}")
        logger.debug(f"{'='*60}\n")
    
    # Обрабатываем список стикеров
    for sticker in stickers:
        if isinstance(sticker, str):
            # Если стикер - это строка (возможно, ID состояния)
            if sticker in STATUS_STATES:
                return STATUS_STATES[sticker]
        elif isinstance(sticker, dict):
            # Если стикер - это объект
            sticker_id = sticker.get("stickerId") or sticker.get("id")
            if sticker_id == STATUS_STICKER_ID:
                state_id = sticker.get("stateId") or sticker.get("state")
                if state_id and state_id in STATUS_STATES:
                    return STATUS_STATES[state_id]
    
    return None

def api_get(endpoint: str, params: Optional[dict] = None) -> dict:
    """GET-запрос с поддержкой заголовка Retry-After для 429."""
    url = f"{BASE}/{endpoint}"
    for attempt in range(6):  # 1 основной + 5 повторов
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429 and attempt < 5:
                delay = int(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt == 5:
                raise RuntimeError(f"Не удалось получить {url}: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Не удалось получить {url}")

def paginate(endpoint: str, key: str, limit: int = PAGE_LIMIT) -> Iterable[dict]:
    """Ленивый генератор, возвращающий элементы постранично."""
    offset = 0
    while True:
        block = api_get(endpoint, {"limit": limit, "offset": offset})
        items = extract_items(block, key)
        if not items:
            break
        yield from items
        paging = block.get("paging", {})
        if not paging.get("next") and len(items) < limit:
            break
        offset += limit

def to_iso(ms: Optional[int]) -> Optional[str]:
    """Преобразует millisecond-timestamp в ISO-8601 UTC или возвращает None."""
    if ms is None:
        return None
    
    # Если это словарь, пытаемся извлечь timestamp
    if isinstance(ms, dict):
        # Возможные ключи для timestamp в словаре
        for key in ['timestamp', 'date', 'value', 'ms']:
            if key in ms:
                ms = ms[key]
                break
        else:
            # Если не нашли подходящий ключ, возвращаем None
            return None
    
    # Если это строка, пытаемся преобразовать в число
    if isinstance(ms, str):
        try:
            ms = int(ms)
        except ValueError:
            return None
    
    # Проверяем, что это число
    if not isinstance(ms, (int, float)):
        return None
    
    try:
        return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"
    except (ValueError, OSError):
        # Обработка случаев с некорректными timestamp
        return None

# ──────────── бизнес-логика ────────────────────────────────────────────

def build_user_dictionaries() -> Dict[str, str]:
    """Строит справочник пользователей."""
    users_resp = api_get("users", {"limit": 1000})
    users = extract_items(users_resp, "users")
    name_by_id = {u["id"]: u["realName"] for u in users}
    return name_by_id

def collect_and_filter_tasks(target_project: str) -> List[dict]:
    """Собирает и фильтрует задачи по проекту, оставляя только те, у которых есть стикер статуса."""
    logger.info("Загрузка всех задач...")
    all_tasks = list(paginate("tasks", "tasks"))
    
    # Убираем подзадачи
    sub_ids = {sid for t in all_tasks for sid in (t.get("subtasks") or [])}
    tasks = [t for t in all_tasks if t["id"] not in sub_ids]
    logger.info(f"Найдено задач (без подзадач): {len(tasks)}")
    
    # Фильтруем задачи со стикером статуса
    tasks_with_status = []
    for t in tasks:
        if get_task_full_status(t) is not None:
            tasks_with_status.append(t)
    
    logger.info(f"Задач со стикером статуса: {len(tasks_with_status)}")
    
    # Загружаем необходимые справочники для фильтрации по проекту
    needed_columns = {t.get("columnId") for t in tasks_with_status if t.get("columnId")}
    columns = {
        c["id"]: c
        for c in paginate("columns", "columns")
        if c["id"] in needed_columns
    }
    
    board_ids = {c["boardId"] for c in columns.values()}
    boards = {b["id"]: b for b in paginate("boards", "boards") if b["id"] in board_ids}
    
    proj_ids = {b["projectId"] for b in boards.values()}
    projects = {
        p["id"]: p for p in paginate("projects", "projects") if p["id"] in proj_ids
    }
    
    # Фильтруем по проекту
    project_filtered = []
    for t in tasks_with_status:
        col = columns.get(t.get("columnId"))
        if not col:
            continue
        board = boards.get(col["boardId"])
        if not board:
            continue
        proj = projects.get(board["projectId"])
        if proj and proj["title"].lower() == target_project.lower():
            # Добавляем контекст для последующей обработки
            t["_column"] = col
            t["_board"] = board
            t["_project"] = proj
            project_filtered.append(t)
    
    logger.info(f"Задач в проекте '{target_project}' со стикером статуса: {len(project_filtered)}")
    return project_filtered

def build_export(tasks: List[dict], name_by_id: Dict[str, str]) -> List[dict]:
    """Формирует экспортные данные из отфильтрованных задач."""
    export: List[dict] = []
    
    for idx, t in enumerate(tasks):
        # Определяем исполнителей
        assigned_users = []
        if t.get("assigned"):
            for user_id in t["assigned"]:
                user_name = name_by_id.get(user_id, user_id)
                assigned_users.append(user_name)
        executor = ", ".join(assigned_users) if assigned_users else "Не назначен"
        
        # Определяем статус
        task_status = "Закрыта" if t.get("completedTimestamp") else "В работе"
        
        # Определяем полный статус из стикера
        full_status = get_task_full_status(t, debug=False)
        
        # Определяем название колонки
        col = t.get("_column")
        column_name = (col.get("title") or col.get("name")) if col else None
        
        # Извлекаем только необходимые данные из raw
        # Временно сохраняем все данные для отладки
        essential_raw = t  # Полные данные для отладки
        
        export_item = {
            "id": t["id"],
            "title": t["title"],
            "description": t.get("description", ""),
            "executor": executor,
            "status": task_status,
            "full_status": full_status or "Нет данных",  # Всегда добавляем поле
            "createdAt": to_iso(t.get("timestamp")),
            "closedAt": to_iso(t.get("completedTimestamp")),
            "column": column_name,
            "priority": t.get("priority"),
            "deadline": to_iso(t.get("deadline")),
            "tags": t.get("tags", []),
            "raw": essential_raw,
        }
        
        export.append(export_item)
    
    return export

# ──────────── точка входа ───────────────────────────────────────────────

def main() -> None:
    """Основная функция с обработкой ошибок."""
    try:
        logger.info("Начало экспорта данных из Yougile")
        
        # Подготовка справочников
        logger.info("Подготовка справочника пользователей...")
        name_by_id = build_user_dictionaries()
        
        # Загрузка и фильтрация задач
        logger.info(f"Загрузка задач для проекта '{TARGET_PROJECT}'...")
        tasks = collect_and_filter_tasks(TARGET_PROJECT)
        
        if not tasks:
            logger.warning("Не найдено задач со стикером статуса в проекте")
            return
        
        # Формирование экспорта
        export = build_export(tasks, name_by_id)
        
        # Сохранение результата
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(
            json.dumps(export, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        
        logger.info(f"Экспорт завершен успешно! Сохранено задач: {len(export)}")
        logger.info(f"Файл: {OUT_PATH}")
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении экспорта: {e}")
        raise

if __name__ == "__main__":
    main()