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

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / 'raw_data'
OUT_DIR.mkdir(exist_ok=True)

# ── конфигурация ───────────────────────────────────────────────────────────
TOKEN   = "Mn0MxS4Sp+BRTXmZG3G3Q3R1+IHVPmXMbvZgrq51QaLCc9h5NxmhJOh0WqUZxqHH"
BASE    = "https://yougile.com/api-v2"
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
        data  = api_get(ep, {"limit": PAGE_LIMIT, "offset": offset})
        items = data.get(key) or data.get("content") or []
        yield from items
        if not data.get("paging", {}).get("next") and len(items) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

# ── пользователи ───────────────────────────────────────────────────────────
users = [{"id": u["id"], "name": u["realName"]}
         for u in paged("users", "content")]

# ── проекты ────────────────────────────────────────────────────────────────
projects = [{"id": p["id"], "title": p["title"]}
            for p in paged("projects", "projects")]

# ── строковые стикеры ───────────────────────────────────────────────────────
stickers = [
    {"stickerId": s["id"], "stateId": st["id"], "stateName": st["name"]}
    for s in paged("string-stickers", "stickers")
    for st in s.get("states", [])
]

# ── сохранение ─────────────────────────────────────────────────────────────
OUT_DIR.mkdir(parents=True, exist_ok=True)

with open(OUT_DIR / "yougile_users.json", "w", encoding="utf-8") as f:
    json.dump(users, f, ensure_ascii=False, indent=2)

with open(OUT_DIR / "yougile_projects.json", "w", encoding="utf-8") as f:
    json.dump(projects, f, ensure_ascii=False, indent=2)

with open(OUT_DIR / "yougile_stickers.json", "w", encoding="utf-8") as f:
    json.dump(stickers, f, ensure_ascii=False, indent=2)

print(f"Файлы сохранены в: {OUT_DIR}")