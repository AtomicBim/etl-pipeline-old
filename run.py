import os
import subprocess
import sys
import pathlib
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

BASE_DIR = pathlib.Path(__file__).resolve().parent
os.environ['ETL_ROOT'] = str(BASE_DIR)

def run_py_scripts(scripts_folder):
    for script in sorted((BASE_DIR / scripts_folder).glob('*.py')):
        print(f'Выполняется {script.name}')
        subprocess.run([sys.executable, str(script)], check=True)

def run_notebooks_in_order(notebook_names):
    datasets_dir = BASE_DIR / 'datasets'
    for name in notebook_names:
        nb_file = datasets_dir / f'create_{name}_dataset.ipynb'
        if nb_file.exists():
            print(f'Выполняется {nb_file.name}')
            with open(nb_file, encoding='utf-8') as f:
                nb = nbformat.read(f, as_version=4)
                ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
                ep.preprocess(nb, {'metadata': {'path': str(nb_file.parent)}})
        else:
            print(f'Не найден: {nb_file}')

if __name__ == "__main__":
    run_py_scripts('extractors')
    run_notebooks_in_order(['scripts', 'gitlab', 'projectsync', 'yougile', 'logs'])
    
    print('ETL pipeline завершен.')