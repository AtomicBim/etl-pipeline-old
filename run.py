import os
import subprocess
import sys
import pathlib
import asyncio
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from config.logging_config import setup_logging
from typing import List, Tuple

# Fix for Windows Proactor event loop warning with ZMQ
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BASE_DIR = pathlib.Path(__file__).resolve().parent
os.environ['ETL_ROOT'] = str(BASE_DIR)

logger = setup_logging(__name__)

def run_py_scripts(scripts_folder: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Запускает Python скрипты из указанной папки.
    
    Returns:
        Tuple[List[str], List[Tuple[str, str]]]: (успешные модули, неуспешные модули с ошибками)
    """
    logger.info(f"Запуск скриптов из папки {scripts_folder}")
    successful = []
    failed = []
    
    scripts = sorted((BASE_DIR / scripts_folder).glob('*.py'))
    if not scripts:
        logger.warning(f"Не найдено скриптов в папке {scripts_folder}")
        return successful, failed
    
    for script in scripts:
        try:
            logger.info(f'Выполняется {script.name}')
            result = subprocess.run([sys.executable, str(script)], 
                                  check=True, 
                                  capture_output=False,  # Позволяем выводу идти напрямую
                                  text=True,
                                  timeout=1200)  # 20 минут timeout
            successful.append(script.name)
            logger.info(f'✓ {script.name} выполнен успешно')
                
        except subprocess.CalledProcessError as e:
            error_msg = f"Код возврата: {e.returncode}"
            failed.append((script.name, error_msg))
            logger.error(f'✗ Ошибка в {script.name}: {error_msg}')
            
        except subprocess.TimeoutExpired as e:
            error_msg = f"Превышен timeout ({e.timeout}s)"
            failed.append((script.name, error_msg))
            logger.error(f'✗ Timeout в {script.name}: {error_msg}')
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            failed.append((script.name, error_msg))
            logger.error(f'✗ Неожиданная ошибка в {script.name}: {error_msg}')
    
    logger.info(f"Завершена обработка скриптов из {scripts_folder}. "
                f"Успешно: {len(successful)}, С ошибками: {len(failed)}")
    return successful, failed

def run_notebooks_in_order(notebook_names: List[str]) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Запускает Jupyter notebooks в указанном порядке.
    
    Returns:
        Tuple[List[str], List[Tuple[str, str]]]: (успешные notebooks, неуспешные notebooks с ошибками)
    """
    logger.info("Запуск обработки датасетов")
    successful = []
    failed = []
    datasets_dir = BASE_DIR / 'datasets'
    
    if not datasets_dir.exists():
        logger.warning(f"Папка датасетов не найдена: {datasets_dir}")
        return successful, failed
    
    for name in notebook_names:
        nb_file = datasets_dir / f'create_{name}_dataset.ipynb'
        if not nb_file.exists():
            error_msg = f"Файл не найден: {nb_file}"
            failed.append((f'create_{name}_dataset.ipynb', error_msg))
            logger.warning(f'✗ {error_msg}')
            continue
            
        try:
            logger.info(f'Выполняется {nb_file.name}')
            with open(nb_file, encoding='utf-8') as f:
                nb = nbformat.read(f, as_version=4)
                # Настраиваем ExecutePreprocessor для вывода логов
                ep = ExecutePreprocessor(
                    timeout=1200, 
                    kernel_name='python3',
                    allow_errors=False,
                    store_widget_state=False,
                    log_output=True  # Включаем вывод логов
                )
                ep.preprocess(nb, {'metadata': {'path': str(nb_file.parent)}})
            
            successful.append(nb_file.name)
            logger.info(f'✓ {nb_file.name} выполнен успешно')
            
        except nbformat.ValidationError as e:
            error_msg = f"Ошибка валидации notebook: {str(e)}"
            failed.append((nb_file.name, error_msg))
            logger.error(f'✗ Ошибка валидации в {nb_file.name}: {error_msg}')
            
        except Exception as e:
            error_msg = f"Ошибка выполнения: {str(e)}"
            failed.append((nb_file.name, error_msg))
            logger.error(f'✗ Ошибка в {nb_file.name}: {error_msg}')
    
    logger.info(f"Завершена обработка notebooks. "
                f"Успешно: {len(successful)}, С ошибками: {len(failed)}")
    return successful, failed

def print_final_report(py_success: List[str], py_failed: List[Tuple[str, str]], 
                      nb_success: List[str], nb_failed: List[Tuple[str, str]]):
    """Выводит финальный отчет о выполнении ETL pipeline"""
    total_success = len(py_success) + len(nb_success)
    total_failed = len(py_failed) + len(nb_failed)
    total_modules = total_success + total_failed
    
    logger.info("=" * 60)
    logger.info("ФИНАЛЬНЫЙ ОТЧЕТ ETL PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Всего модулей: {total_modules}")
    logger.info(f"Успешно выполнено: {total_success}")
    logger.info(f"С ошибками: {total_failed}")
    
    if py_success:
        logger.info(f"\n✓ Успешные Python скрипты ({len(py_success)}):")
        for script in py_success:
            logger.info(f"  - {script}")
    
    if nb_success:
        logger.info(f"\n✓ Успешные Jupyter notebooks ({len(nb_success)}):")
        for notebook in nb_success:
            logger.info(f"  - {notebook}")
    
    if py_failed or nb_failed:
        logger.warning(f"\n✗ Модули с ошибками ({total_failed}):")
        for name, error in py_failed + nb_failed:
            logger.warning(f"  - {name}: {error}")
    
    if total_failed == 0:
        logger.info("\n🎉 Все модули ETL pipeline выполнены успешно!")
    else:
        success_rate = round((total_success / total_modules) * 100, 1) if total_modules > 0 else 0
        logger.warning(f"\n⚠️  Pipeline завершен с ошибками. Успешность: {success_rate}%")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    logger.info("ETL pipeline запущен")
    
    try:
        # Запуск Python скриптов
        py_success, py_failed = run_py_scripts('extractors')
        
        # Запуск Jupyter notebooks
        nb_success, nb_failed = run_notebooks_in_order(['scripts', 'gitlab', 'projectsync', 'yougile', 'logs'])
        
        # Финальный отчет
        print_final_report(py_success, py_failed, nb_success, nb_failed)
        
        # Определяем общий статус завершения
        total_failed = len(py_failed) + len(nb_failed)
        if total_failed == 0:
            logger.info('ETL pipeline завершен успешно')
        else:
            logger.warning(f'ETL pipeline завершен с {total_failed} ошибками')
            
    except KeyboardInterrupt:
        logger.warning("ETL pipeline прерван пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Критическая ошибка в ETL pipeline: {e}")
        sys.exit(1)