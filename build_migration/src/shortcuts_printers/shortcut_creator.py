"""
Модуль для создания ярлыков.

Функции:
    - parse_links_file: Парсинг файла links.txt для создания ярлыков.
    - create_shortcuts: Создание ярлыков на основе списка.
"""
import os
import logging
import stat
import re
from typing import List, Tuple
from urllib.parse import urlparse
from src.config.config_loader import load_config
from src.logging.logger import setup_logger

# Настройка логгера
logger = logging.getLogger(__name__)

def convert_windows_path_to_linux(windows_path: str, username: str, additional_disk_mapping=None) -> str:
    """
    Конвертирование пути Windows в путь Linux.

    :param windows_path: Путь Windows.
    :param username: Имя пользователя.
    :param additional_disk_mapping: Дополнительное соответствие букв дисков
    :return: Путь Linux.
    """
    path = windows_path.replace('\\', '/')
    user_windows_path = f'C:/Users/{username}'
    user_linux_path = f'/home/{username}'

    if path.startswith(user_windows_path):
        path = path.replace(user_windows_path, user_linux_path)
    else:
        if additional_disk_mapping:
            matched = False
            for windows_disk, linux_mount_point in additional_disk_mapping.items():
                windows_disk_prefix = f'{windows_disk.upper()}:/'
                if path.upper().startswith(windows_disk_prefix):
                    path = path.replace(windows_disk_prefix, linux_mount_point)
                    matched = True
                    break
            if not matched:
                logger.warning(f"Диск {path[:2]} не найден в additional_disk_mapping.")
        else:
            if path.startswith('C:/'):
                path = path.replace('C:/', '/')
            # Добавьте обработку других дисков по необходимости
    return path




def sanitize_filename(name: str) -> str:
    # Заменяем недопустимые символы на подчеркивания
    return re.sub(r'[<>:"/\\|?*]', '_', name)


def escape_desktop_entry_value(value: str) -> str:
    # Экранируем кавычки и другие специальные символы
    return value.replace('"', '\\"')


def is_url(target: str) -> bool:
    # Проверяем, является ли строка URL-адресом
    parsed = urlparse(target)
    return parsed.scheme in ('http', 'https', 'ftp', 'ftps')


def create_shortcuts(shortcuts: List[Tuple[str, str]], user_dir: str, username: str, additional_disk_mapping=None) -> None:
    """
    Создание ярлыков на основе списка.

    :param shortcuts: Список ярлыков (имя, путь).
    :param user_dir: Директория пользователя.
    """
    if not os.path.exists(user_dir):
        try:
            os.makedirs(user_dir)
        except Exception as e:
            logger.error(f"Не удалось создать директорию пользователя {user_dir}: {e}")
            return

    for index, item in enumerate(shortcuts, 1):
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            logger.error(f"Некорректный формат ярлыка: {item}")
            continue

        name, target = item
        safe_name = sanitize_filename(name)
        shortcut_path = os.path.join(user_dir, f"{safe_name}.desktop")

        name_escaped = escape_desktop_entry_value(name)

        if is_url(target):
            target_converted = target
            desktop_entry_content = f"""[Desktop Entry]
Name={name_escaped}
Type=Link
URL={target_converted}
Icon=internet-web-browser
"""
        else:
            # Преобразуем путь из Windows в Linux
            target_converted = convert_windows_path_to_linux(target, username, additional_disk_mapping=additional_disk_mapping)

            # Обработка сетевых путей Windows
            if target_converted.startswith(('//', '\\\\')):
                target_converted = 'smb:' + target_converted.replace('//', '/').replace('\\', '/')

            target_escaped = escape_desktop_entry_value(target_converted)

            desktop_entry_content = f"""[Desktop Entry]
Name={name_escaped}
Type=Application
Exec=xdg-open "{target_escaped}"
Icon=folder
"""

            # Проверяем существование локального пути
            if not is_url(target_converted) and not target_converted.startswith('smb:') and not os.path.exists(target_converted):
                logger.warning(f"Целевой путь {target_converted} не существует или недоступен.")

        try:
            with open(shortcut_path, 'w', encoding='utf-8') as file:
                file.write(desktop_entry_content)
            os.chmod(shortcut_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            logger.info(f"[{index}/{len(shortcuts)}] Ярлык {shortcut_path} создан для {target_converted}.")
        except Exception as e:
            logger.error(f"Ошибка при создании ярлыка {shortcut_path}: {e}")

