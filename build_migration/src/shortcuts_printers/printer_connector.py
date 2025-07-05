"""
Модуль для подключения сетевых принтеров.
Функции:
	- connect_printers: Подключение сетевых принтеров на основе информации из файла.
"""
import subprocess
import logging
from src.config.config_loader import load_config
from src.logging.logger import setup_logger

# Настройка логгера
setup_logger()
logger = logging.getLogger(__name__)
config = load_config()

def connect_printers(printer_info_file):
	"""
	Подключение сетевых принтеров на основе информации из файла.

	:param printer_info_file: Путь к файлу с информацией о принтерах.
	"""
	try:
		with open(printer_info_file, 'r') as file:
			printers = file.readlines()

		for printer in printers:
			# Удаляем возможные пробелы и пустые строки
			printer = printer.strip()
			if not printer:
				continue

			# Разделяем строку на имя принтера и URL
			parts = printer.split(';', maxsplit=1)
			if len(parts) != 2:
				logger.error(f"Ошибка в формате строки: {printer}")
				continue

			printer_name, printer_url = parts

			# Команда для подключения принтера
			cmd = [
				'lpadmin',
				'-p', printer_name,
				'-D', printer_name,
				'-E',
				'-v', printer_url,
				'-m', 'drv:///sample.drv/generpcl.ppd',
				'-o', 'printer-is-shared=false'
			]

			# Выполняем команду
			try:
				subprocess.run(cmd, check=True)
				logger.info(f"Принтер '{printer_name}' успешно подключен.")
			except subprocess.CalledProcessError as e:
				logger.error(f"Ошибка подключения принтера '{printer_name}': {e}")

	except FileNotFoundError:
		logger.warning(f"Файл с информацией о принтерах не найден: {printer_info_file}")
	except Exception as e:
		logger.warning(f"Ошибка при обработке файла с информацией о принтерах: {e}")

