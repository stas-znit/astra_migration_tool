"""
Модуль для настройки логгера.

Функции:
    - setup_logger: Настройка логгера для приложения.
"""
import logging
from logging.handlers import RotatingFileHandler
import os
from src.config.config_loader import load_config

def setup_logger():
    """
    Настройка логгера для приложения.

    """
    config = load_config()
    log_file = config.get("LOG_FILES", "/tmp/default.log")
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    try:

        # Настройка логгера
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        if not logger.handlers:

            # Создание форматтера
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # Обработчик для записи в файл
            file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)

            # Обработчик для вывода в консоль
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)

            # Добавление обработчиков к логгеру
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

            # Если требуется, убрать дублирование логов
            logger.propagate = False

    except Exception as e:
        print(f"Произошла ошибка при настройке логгера: {e}")

