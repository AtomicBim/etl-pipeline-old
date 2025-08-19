#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ускоренный экспорт статистики строк кода из GitLab‑проектов.

Оптимизации:
  • один shallow‑clone на проект (--no-single-branch) вместо клонирования каждой ветки;
  • чтение содержимого файлов прямо из объекта Git (git show) без checkout на диск;
  • параллельная обработка проектов ThreadPoolExecutor;
  • минимальное число API‑запросов к GitLab.
"""
from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

import gitlab
from git import Repo
from tqdm import tqdm
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

# ─────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────
GITLAB_URL: str = "http://192.168.42.188:13080"
BASE_DIR: Path = Path(__file__).resolve().parent.parent
TOKENS_PATH: Path = BASE_DIR / "config" / "tokens.json"
OUT_JSON: Path = BASE_DIR / "raw_data" / "gitlab_export_lines.json"

INCLUDE_EXTS: dict[str, str] = {
    ".cs": "C#",
    ".py": "Python",
    ".xaml": "XAML",
    ".yml": "YAML",
    ".yaml": "YAML",
    ".html": "HTML",
    ".htm": "HTML",
    ".ts": "TypeScript",
    ".tsx": "TypeScript",
    ".js": "JavaScript",
    ".jsx": "JavaScript",
    ".css": "CSS",
    ".scss": "SCSS",
    ".sass": "SASS",
    ".sln": "Solution",
}

BRACE_ONLY: set[str] = {"{", "}"}  # одинокие фигурные скобки
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "8"))

# ─────────────────────────────────────────────
# Авторизация
# ─────────────────────────────────────────────
PRIVATE_TOKEN: str = json.loads(TOKENS_PATH.read_text(encoding="utf-8"))["gitlab"]["token"]
gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN, keep_base_url=True)
gl.auth()

logger = setup_logging(__name__)

# ─────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────

def url_with_token(url: str) -> str:
    """Встраивает private_token (PAT) в URL репозитория."""
    proto, rest = (
        ("http://", url.removeprefix("http://"))
        if url.startswith("http://")
        else ("https://", url.removeprefix("https://"))
    )
    if "@" in rest.split("/", 1)[0]:  # убрать git@host
        rest = rest.split("@", 1)[1]
    return f"{proto}oauth2:{PRIVATE_TOKEN}@{rest}"


def list_branches(pr) -> set[str]:
    """Возвращает master, default и все ветки, содержащие 'dev'."""
    branches: set[str] = {pr.default_branch or "main", "master"}
    for br in pr.branches.list(iterator=True, per_page=100):
        if "dev" in br.name.lower():
            branches.add(br.name)
    return branches


def branch_loc(repo: Repo, ref: str) -> Dict[str, object] | None:
    """Считает строки кода на указанном ref (ветка/коммит)."""
    try:
        paths = repo.git.ls_tree("-r", "--name-only", ref).splitlines()
    except Exception:
        return None

    lang_loc: dict[str, int] = defaultdict(int)
    for path in paths:
        ext = Path(path).suffix.lower()
        if ext not in INCLUDE_EXTS:
            continue
        try:
            blob = repo.git.show(f"{ref}:{path}")
        except Exception:
            continue

        lang = INCLUDE_EXTS[ext]
        cs_file = ext == ".cs"
        for line in blob.splitlines():
            line = line.strip()
            if not line:
                continue
            if cs_file and line in BRACE_ONLY:
                continue
            lang_loc[lang] += 1

    if not lang_loc:
        return None
    return {"loc": sum(lang_loc.values()), "langs": lang_loc}


def process_project(pr) -> Dict[str, object]:
    """Обрабатывает проект GitLab, возвращая данные для JSON отчёта."""
    with tempfile.TemporaryDirectory() as tmp:
        try:
            repo = Repo.clone_from(
                url_with_token(pr.http_url_to_repo),
                tmp,
                depth=1,
                no_single_branch=True,  # один набор объектов на все ветки
                quiet=True,
            )
        except Exception:
            return {
                "id": pr.id,
                "name": pr.path_with_namespace,
                "chosen_branch": None,
                "loc_by_language": {},
            }

        branch_stats: Dict[str, Dict[str, object]] = {}
        for br in list_branches(pr):
            if br in branch_stats:
                continue
            stat = branch_loc(repo, br)
            if stat:
                branch_stats[br] = stat

        if not branch_stats:
            return {
                "id": pr.id,
                "name": pr.path_with_namespace,
                "chosen_branch": None,
                "loc_by_language": {},
            }

        chosen_branch = max(branch_stats.items(), key=lambda kv: kv[1]["loc"])[0]
        return {
            "id": pr.id,
            "name": pr.path_with_namespace,
            "chosen_branch": chosen_branch,
            "loc_by_language": branch_stats[chosen_branch]["langs"],
        }

# ─────────────────────────────────────────────
# Основная точка входа
# ─────────────────────────────────────────────

def main() -> None:
    logger.info("Начало экспорта статистики строк кода из GitLab проектов")
    projects = list(gl.projects.list(iterator=True, per_page=100))
    result: List[Dict[str, object]] = []

    logger.info(f"Обработка {len(projects)} проектов...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_project, pr): pr.id for pr in projects}
        
        progress_bar = tqdm(
            as_completed(futures), 
            total=len(futures), 
            desc="Projects", 
            unit="proj"
        )
        
        for i, fut in enumerate(progress_bar):
            result.append(fut.result())

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Экспорт завершен. Сохранено: {OUT_JSON}")


if __name__ == "__main__":
    main()