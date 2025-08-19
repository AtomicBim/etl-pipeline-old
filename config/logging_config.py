import logging
import sys
from pathlib import Path

def setup_logging(module_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Настраивает логирование для модулей ETL pipeline.
    
    Args:
        module_name: Имя модуля для логирования
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        logging.Logger: Настроенный логгер
    """
    logger = logging.getLogger(module_name)
    
    # Избегаем дублирования обработчиков
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Устанавливаем уровень логирования
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger