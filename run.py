import os
import subprocess
import sys
import pathlib
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from config.logging_config import setup_logging

BASE_DIR = pathlib.Path(__file__).resolve().parent
os.environ['ETL_ROOT'] = str(BASE_DIR)

logger = setup_logging(__name__)

def run_py_scripts(scripts_folder):
    logger.info(f"Запуск скриптов из папки {scripts_folder}")
    for script in sorted((BASE_DIR / scripts_folder).glob('*.py')):
        logger.info(f'Выполняется {script.name}')
        subprocess.run([sys.executable, str(script)], check=True)

def run_notebooks_in_order(notebook_names):
    logger.info("Запуск обработки датасетов")
    datasets_dir = BASE_DIR / 'datasets'
    for name in notebook_names:
        nb_file = datasets_dir / f'create_{name}_dataset.ipynb'
        if nb_file.exists():
            logger.info(f'Выполняется {nb_file.name}')
            with open(nb_file, encoding='utf-8') as f:
                nb = nbformat.read(f, as_version=4)
                ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
                ep.preprocess(nb, {'metadata': {'path': str(nb_file.parent)}})
        else:
            logger.warning(f'Не найден: {nb_file}')

if __name__ == "__main__":
    logger.info("ETL pipeline запущен")
    run_py_scripts('extractors')
    run_notebooks_in_order(['scripts', 'gitlab', 'projectsync', 'yougile', 'logs'])
    logger.info('ETL pipeline завершен')