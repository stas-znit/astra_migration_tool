"""
Модуль для парсинга файла links.txt для создания ярлыков.

Функции:
	- parse_links_file: Парсинг файла links.txt для создания ярлыков.
"""
import os
import logging
from typing import List, Tuple
from collections import namedtuple
from src.config.config_loader import load_config
from src.logging.logger import setup_logger

# Настройка логгера
logger = logging.getLogger(__name__)
config = load_config()

# Определение кортежа
Shortcut = namedtuple('Shortcut', ['name', 'path'])
def parse_links_file(user_dir : str) -> List[Shortcut]:
	"""
	Парсинг файла links.txt для создания ярлыков.

	:param user_dir: Путь к директории пользователя.
	:return: Список ярлыков.

	Формат файла links.txt:
    Каждая строка содержит название ярлыка и путь, разделенные символом '|'.
    Пример строки:
    Мой компьютер|C:\...\
	"""
	links_file = os.path.join(user_dir, config["SHORTCUTS_FILE_NAME"])
	if not os.path.exists(links_file):
		logger.warning(f"Файл {links_file} не найден.")
		return []

	shortcuts = []
	try:
		with open(links_file, 'r', encoding='utf-8') as f:
			for line_number, line in enumerate(f, start=1):
				parts = line.strip().split(sep='|', maxsplit=1)
				if len(parts) == 2:
					shortcuts.append(Shortcut(name = parts[0], path=parts[1]))
				else:
					logger.warning(f"Некорректная строка в {links_file}: на линии {line_number}: {line.strip()}")
	except Exception as e:
		logger.warning(f"Ошибка при чтении {links_file}: {e}")
		return []

	return shortcuts
