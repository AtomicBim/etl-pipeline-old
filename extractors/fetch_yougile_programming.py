#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
import datetime as dt
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from pathlib import Path

# ──────────── конфигурация ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_PATH = OUT_DIR / 'yougile_export_programming.json'

TOKEN = "Mn0MxS4Sp+BRTXmZG3G3Q3R1+IHVPmXMbvZgrq51QaLCc9h5NxmhJOh0WqUZxqHH"

BASE = "https://yougile.com/api-v2"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

TARGET_PROJECT = "Разработка"
PAGE_LIMIT = 1000

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
        print(f"\n{'='*60}")
        print(f"Debug stickers for task: {task.get('title', 'Unknown')}")
        print(f"Task ID: {task.get('id', 'unknown')}")
        print(f"Original stickers type: {type(task.get('stickers'))}")
        print(f"Processed stickers type: {type(stickers)}")
        print(f"Stickers count: {len(stickers) if isinstance(stickers, list) else 'N/A'}")
        
        if isinstance(stickers, list):
            for i, sticker in enumerate(stickers):
                print(f"\nSticker {i+1}:")
                print(f"  Type: {type(sticker)}")
                if isinstance(sticker, dict):
                    print(f"  Keys: {list(sticker.keys())}")
                    print(f"  Content: {sticker}")
                else:
                    print(f"  Value: {sticker}")
        
        # Также показываем оригинальную структуру stickers
        print(f"\nOriginal stickers data:")
        print(f"{task.get('stickers')}")
        print(f"{'='*60}\n")
    
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
    print("Загрузка всех задач...")
    all_tasks = list(paginate("tasks", "tasks"))
    
    # Убираем подзадачи
    sub_ids = {sid for t in all_tasks for sid in (t.get("subtasks") or [])}
    tasks = [t for t in all_tasks if t["id"] not in sub_ids]
    print(f"Найдено задач (без подзадач): {len(tasks)}")
    
    # Фильтруем задачи со стикером статуса
    tasks_with_status = []
    for t in tasks:
        if get_task_full_status(t) is not None:
            tasks_with_status.append(t)
    
    print(f"Задач со стикером статуса: {len(tasks_with_status)}")
    
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
    
    print(f"Задач в проекте '{target_project}' со стикером статуса: {len(project_filtered)}")
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
        print("🚀 Начало экспорта данных из Yougile")
        print("=" * 50)
        
        # Подготовка справочников
        print("📚 Подготовка справочника пользователей...")
        name_by_id = build_user_dictionaries()
        
        # Загрузка и фильтрация задач
        print(f"\n📋 Загрузка задач для проекта '{TARGET_PROJECT}'...")
        tasks = collect_and_filter_tasks(TARGET_PROJECT)
        
        if not tasks:
            print("❌ Не найдено задач со стикером статуса в проекте")
            return
        
        # Формирование экспорта
        export = build_export(tasks, name_by_id)
        
        # Сохранение результата
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(
            json.dumps(export, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        
        print("\n✅ Экспорт завершен успешно!")
        print(f"📊 Сохранено задач: {len(export)}")
        print(f"📁 Файл: {OUT_PATH}")
        
        # Статистика по полным статусам
        full_statuses = {}
        for task in export:
            status = task["full_status"]
            full_statuses[status] = full_statuses.get(status, 0) + 1
        
        # Статистика по колонкам
        columns = {}
        for task in export:
            col = task.get("column", "Не указана")
            columns[col] = columns.get(col, 0) + 1
            
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении экспорта: {e}")
        raise

if __name__ == "__main__":
    main()

# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# from __future__ import annotations

# import json
# import time
# import datetime as dt
# from typing import Dict, Iterable, List

# import requests
# from requests.adapters import HTTPAdapter, Retry
# from pathlib import Path

# BASE_DIR = Path(__file__).resolve().parent.parent
# OUT_DIR = BASE_DIR / 'raw_data'
# OUT_DIR.mkdir(exist_ok=True)
# OUT_PATH = OUT_DIR / 'yougile_export_programming.json'

# # ──────────── конфигурация ──────────────────────────────────────────────
# TOKEN = "Mn0MxS4Sp+BRTXmZG3G3Q3R1+IHVPmXMbvZgrq51QaLCc9h5NxmhJOh0WqUZxqHH"
# BASE = "https://yougile.com/api-v2"
# HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# DISCIPLINE_PREFIXES = ["АР", "КЖ", "ВК/ОВ", "ЭЛ", "Общие", "Системные"]
# TARGET_PROJECT = "Разработка"
# PAGE_LIMIT = 1000
# TARGET_USERS = [
#     "Андрей Кичигин",
#     "Александр Андреев",
#     "Роман Урманчеев",
#     "Ольга Кузовлева",
#     "Анна Романова"
# ]

# # ──────────── сессия HTTP с повторениями ────────────────────────────────
# session = requests.Session()
# session.headers.update(HEADERS)
# # автоматические повторы для сетевых ошибок и 5xx/429 (кроме логики Retry-After)
# retry_cfg = Retry(
#     total=5,
#     backoff_factor=1,
#     status_forcelist=(429, 500, 502, 503, 504),
#     allowed_methods={"GET"},
# )
# session.mount("https://", HTTPAdapter(max_retries=retry_cfg))

# # ──────────── вспомогательные функции ───────────────────────────────────

# def resolve_discipline(col_title: str | None) -> str | None:
#     """Возвращает название дисциплины по префиксу колонки или None."""
#     if not col_title:
#         return None
#     title = col_title.strip()                       # уберём пробелы по краям
#     for prefix in DISCIPLINE_PREFIXES:
#         if title.startswith(prefix):
#             return prefix
#     return None

# def api_get(endpoint: str, params: dict | None = None) -> dict:
#     """GET-запрос с поддержкой заголовка Retry-After для 429."""
#     url = f"{BASE}/{endpoint}"
#     for attempt in range(6):  # 1 основной + 5 повторов
#         resp = session.get(url, params=params, timeout=30)
#         if resp.status_code == 429 and attempt < 5:
#             delay = int(resp.headers.get("Retry-After", 2 ** attempt))
#             time.sleep(delay)
#             continue
#         resp.raise_for_status()
#         return resp.json()
#     raise RuntimeError(f"Не удалось получить {url}")

# def paginate(endpoint: str, key: str, limit: int = PAGE_LIMIT) -> Iterable[dict]:
#     """Ленивый генератор, возвращающий элементы списком key постранично."""
#     offset = 0
#     while True:
#         block = api_get(endpoint, {"limit": limit, "offset": offset})
#         items: List[dict] = block.get(key) or block.get("content") or []
#         if not items:
#             break
#         yield from items
#         paging = block.get("paging", {})
#         if not paging.get("next") and len(items) < limit:
#             break
#         offset += limit

# def to_iso(ms: int | None) -> str | None:
#     """Преобразует millisecond-timestamp в ISO-8601 UTC или возвращает None."""
#     return (
#         dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="seconds") + "Z"
#         if ms
#         else None
#     )

# # ──────────── бизнес-логика ────────────────────────────────────────────

# def build_reference_dictionaries() -> tuple[Dict[str, str], Dict[str, str], set[str]]:
#     users_resp = api_get("users", {"limit": 1000})
#     users = users_resp.get("users") or users_resp.get("content") or []

#     id_by_name = {u["realName"]: u["id"] for u in users}
#     name_by_id = {u["id"]: u["realName"] for u in users}

#     target_ids = {id_by_name[n] for n in TARGET_USERS if n in id_by_name}
#     missing = [n for n in TARGET_USERS if n not in id_by_name]
#     if missing:
#         print("Не найдены в системе:", ", ".join(missing))

#     return id_by_name, name_by_id, target_ids

# def build_status_dictionaries() -> tuple[str | None, Dict[str, str]]:
#     stickers_raw = list(paginate("string-stickers", "stickers"))
#     status_sticker_id: str | None = None
#     state_name_by_id: Dict[str, str] = {}

#     for sticker in stickers_raw:
#         if sticker.get("name") == "Статус":
#             status_sticker_id = sticker["id"]
#         for state in sticker.get("states", []):
#             state_name_by_id[state["id"]] = state["name"]

#     if status_sticker_id is None:
#         print("Стикер «Статус» не найден — поле status будет null.")

#     return status_sticker_id, state_name_by_id

# def build_board_dictionaries(needed_column_ids: set[str]) -> tuple[Dict[str, dict], Dict[str, dict], Dict[str, dict]]:
#     columns = {
#         c["id"]: c
#         for c in paginate("columns", "columns")
#         if c["id"] in needed_column_ids
#     }
#     board_ids = {c["boardId"] for c in columns.values()}
#     boards = {b["id"]: b for b in paginate("boards", "boards") if b["id"] in board_ids}
#     proj_ids = {b["projectId"] for b in boards.values()}
#     projects = {
#         p["id"]: p for p in paginate("projects", "projects") if p["id"] in proj_ids
#     }
#     return columns, boards, projects

# def collect_tasks(target_ids: set[str]) -> List[dict]:
#     """Собирает и фильтрует задачи, убирая подзадачи и лишних исполнителей."""
#     all_tasks = list(paginate("tasks", "tasks"))

#     # убрать подзадачи
#     sub_ids = {sid for t in all_tasks for sid in (t.get("subtasks") or [])}
#     tasks = [t for t in all_tasks if t["id"] not in sub_ids]

#     # оставить задачи нужных сотрудников
#     return [
#         t
#         for t in tasks
#         if t.get("assigned") and target_ids.intersection(t["assigned"])
#     ]

# def build_export(tasks: List[dict], columns: Dict[str, dict], boards: Dict[str, dict], projects: Dict[str, dict], target_ids: set[str], name_by_id: Dict[str, str]) -> List[dict]:
#     export: List[dict] = []

#     for t in tasks:
#         col = columns.get(t.get("columnId"))
#         board = boards.get(col["boardId"]) if col else None
#         proj = projects.get(board["projectId"]) if board else None
#         if not (proj and proj["title"].lower() == TARGET_PROJECT.lower()):
#             continue

#         exec_id = next((uid for uid in t["assigned"] if uid in target_ids), None)
#         exec_name = name_by_id.get(exec_id, "—")

#         # статус: если есть completedTimestamp → Закрыта, иначе В работе
#         task_status: str = "Закрыта" if t.get("completedTimestamp") else "В работе"

#         discipline = (col.get("title") or col.get("name")) if col else None

#         export.append(
#             {
#                 "id": t["id"],
#                 "title": t["title"],
#                 "description": t.get("description", ""),
#                 "executor": exec_name,
#                 "status": task_status,
#                 "createdAt": to_iso(t.get("timestamp")),
#                 "closedAt": to_iso(t.get("completedTimestamp")),
#                 "discipline": discipline,
#                 "raw": t,  # все стикеры остаются внутри
#             }
#         )

#     return export

# # ──────────── точка входа ───────────────────────────────────────────────

# def main() -> None:
#     print("Подготовка справочников пользователей…")
#     _id_by_name, name_by_id, target_ids = build_reference_dictionaries()

#     print("Загрузка и фильтрация задач…")
#     tasks = collect_tasks(target_ids)

#     needed_columns = {t.get("columnId") for t in tasks if t.get("columnId")}
#     columns, boards, projects = build_board_dictionaries(needed_columns)

#     print("Формирование итоговых данных…")
#     export = build_export(
#         tasks,
#         columns,
#         boards,
#         projects,
#         target_ids,
#         name_by_id,
#     )

#     OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     OUT_PATH.write_text(
#         json.dumps(export, ensure_ascii=False, indent=2),
#         encoding="utf-8",
#     )

#     print(f"Сохранено задач: {len(export)}")
#     print(f"Файл: {OUT_PATH}")

# if __name__ == "__main__":
#     main()    