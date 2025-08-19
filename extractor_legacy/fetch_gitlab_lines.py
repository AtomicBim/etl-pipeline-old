# -*- coding: utf-8 -*-
"""Экспорт всех проектов GitLab в JSON
   со счётчиком строк кода в активной ветке (.cs | .xaml).
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Dict, List
import sys
import os

import gitlab
from git import Repo
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.logging_config import setup_logging

# ───────────────────────────────────────────────────────────────────────
# Константы
# ───────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent.parent
OUT_DIR    = BASE_DIR / "raw_data";                OUT_DIR.mkdir(exist_ok=True)
TOKEN_PATH = BASE_DIR / "config" / "gitlab_token.json"
GITLAB_URL = "http://192.168.42.188:13080"
TARGET_EXTS = {".cs", ".xaml"}

# байтовые представления одиночных фигурных скобок
BRACE_ONLY_B = {b"{", b"}"}

with open(TOKEN_PATH, encoding="utf-8") as f:
    PRIVATE_TOKEN = json.load(f)["gitlab_token"]

logger = setup_logging(__name__)

gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN, keep_base_url=True)
gl.auth()  # проверяем токен

# ───────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ───────────────────────────────────────────────────────────────────────
def embed_token_in_url(repo_http_url: str, token: str) -> str:
    """Вставляем токен в http‑URL для Repo.clone_from."""
    scheme = "http://" if repo_http_url.startswith("http://") else "https://"
    return repo_http_url.replace(scheme, f"{scheme}oauth2:{token}@", 1)

def is_text_file(path: Path, blocksize: int = 1024) -> bool:
    """Бинарные файлы содержат нулевые байты → не считаем их."""
    try:
        return b"\0" not in path.read_bytes()[:blocksize]
    except Exception:
        return False  # не читается → считаем бинарным

def is_brace_only(line: bytes) -> bool:
    """True, если строка состоит только из символов «{» или «}» и пробелов."""
    return line.strip() in BRACE_ONLY_B

def count_lines(repo_dir: Path) -> int:
    """Сумма строк только в .cs/.xaml, без одиноких фигурных скобок в .cs."""
    total = 0
    for file in repo_dir.rglob("*"):
        if (
            file.is_file()
            and file.suffix.lower() in TARGET_EXTS
            and is_text_file(file)
        ):
            cs_file = file.suffix.lower() == ".cs"
            with file.open("rb") as fh:
                for line in fh:
                    if cs_file and is_brace_only(line):
                        continue          # пропускаем «{» / «}»
                    total += 1
    return total

def project_line_count(project: gitlab.v4.objects.Project) -> int:
    """Клонирует default_branch (depth=1) и возвращает число строк."""
    branch = project.default_branch or "master"
    repo_url = embed_token_in_url(project.http_url_to_repo, PRIVATE_TOKEN)

    with tempfile.TemporaryDirectory() as tmp:
        Repo.clone_from(
            repo_url, tmp, branch=branch,
            depth=1, single_branch=True, quiet=True
        )
        return count_lines(Path(tmp))

# ───────────────────────────────────────────────────────────────────────
# Основная логика
# ───────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Запуск экспорта строк кода GitLab проектов (legacy версия)")
    
    try:
        projects = gl.projects.list(all=True)  # без пагинации
        result: List[Dict[str, object]] = []
        
        logger.info(f"Обработка {len(projects)} проектов...")

        for project in tqdm(projects, desc="Сбор метрик", unit="proj"):
            try:
                lines = project_line_count(project)
            except Exception as exc:
                logger.warning(f"{project.path_with_namespace}: {exc}")
                lines = None

            result.append(
                {
                    "id": project.id,
                    "name": project.name,
                    "full_name": project.path_with_namespace,
                    "default_branch": project.default_branch,
                    "lines_of_code": lines,
                }
            )

        out_path = OUT_DIR / "gitlab_export_lines.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Экспорт завершен успешно. Сохранено {len(result)} проектов → {out_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при экспорте GitLab проектов: {e}")
        raise

if __name__ == "__main__":
    main()