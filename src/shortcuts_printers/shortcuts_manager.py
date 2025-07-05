"""
Модуль для управления ярлыками: парсинг файла links.txt и создание .desktop файлов в Linux.

Логика работы:
1. Парсинг файла links.txt из Windows (формат: target|shortcut_path)
2. Извлечение имени ярлыка из пути Windows
3. Конвертация путей Windows в пути Linux
4. Создание .desktop файлов для Linux
5. Поддержка URL, локальных файлов, UNC путей и исполняемых файлов

Функции:
    - parse_links_file: Парсинг файла links.txt
    - convert_windows_path_to_linux: Конвертация путей Windows->Linux
    - create_shortcuts: Создание .desktop файлов
    - process_user_shortcuts: Главная функция обработки ярлыков пользователя
"""

import os
import logging
import stat
import re
from typing import List, Tuple, Dict, Optional
from urllib.parse import urlparse
from collections import namedtuple
from src.config.config_loader import load_config

# Настройка логгера
logger = logging.getLogger(__name__)
config = load_config()

# Структура данных для ярлыка
Shortcut = namedtuple('Shortcut', ['name', 'target'])

class ShortcutsStats:
    """Класс для хранения статистики создания ярлыков"""
    def __init__(self):
        self.successful = 0
        self.failed = 0
        self.warnings = 0
        self.details = []
    
    def add_success(self, name: str):
        self.successful += 1
        self.details.append(f"✓ {name}")
    
    def add_failure(self, name: str, error: str):
        self.failed += 1
        self.details.append(f"✗ {name}: {error}")
    
    def add_warning(self, name: str, warning: str):
        self.warnings += 1
        self.details.append(f"⚠ {name}: {warning}")
    
    def summary(self) -> str:
        return f"Успешно: {self.successful}, Ошибок: {self.failed}, Предупреждений: {self.warnings}"



def parse_links_file(user_dir: str) -> List[Shortcut]:
    """
    Парсинг файла links.txt для создания ярлыков.
    
    Формат файла: target|shortcut_path
    Где target - это что открывает ярлык, shortcut_path - путь к .lnk файлу
    
    :param user_dir: Путь к директории пользователя с файлом links.txt
    :return: Список объектов Shortcut(name, target)
    """
    links_file = os.path.join(user_dir, config["SHORTCUTS_FILE_NAME"])
    
    if not os.path.exists(links_file):
        logger.warning(f"Файл ярлыков {links_file} не найден.")
        return []

    shortcuts = []
    
    try:
        with open(links_file, 'r', encoding='utf-8') as f:
            content = f.read()
            # Убираем BOM если есть
            if content.startswith('\ufeff'):
                content = content[1:]
            content = content.replace('\r\n', '\n').replace('\r', '\n')
            
            lines = content.split('\n')
            
            for line_number, line in enumerate(lines, start=1):
                line = line.strip()
                
                # Пропускаем пустые строки
                if not line:
                    continue
                
                # Парсинг формата: target|shortcut_path
                if '|' not in line:
                    logger.warning(f"Строка {line_number}: отсутствует разделитель '|' - {line}")
                    continue
                
                parts = line.split('|', 1)  # Разделяем только по первому |
                if len(parts) != 2:
                    logger.warning(f"Строка {line_number}: неверный формат - {line}")
                    continue
                
                target, shortcut_path = parts
                target = target.strip()
                shortcut_path = shortcut_path.strip()
                
                # Проверяем, что target не пустой
                if not target:
                    logger.warning(f"Строка {line_number}: пустой target - {line}")
                    continue
                
                # Извлекаем имя ярлыка из shortcut_path 
                shortcut_name = _extract_shortcut_name_from_path(shortcut_path)
                
                if not shortcut_name:
                    shortcut_name = f"shortcut_{line_number}"
                    logger.warning(f"Строка {line_number}: Не удалось извлечь имя ярлыка, используется: {shortcut_name}")
                
                shortcuts.append(Shortcut(name=shortcut_name, target=target))
                logger.debug(f"Строка {line_number}: '{shortcut_name}' -> '{target}'")
                
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {links_file}: {e}")
        return []

    logger.info(f"Найдено {len(shortcuts)} ярлыков в файле {links_file}")
    return shortcuts


def _extract_shortcut_name_from_path(shortcut_path: str) -> str:
    """
    Извлечение имени ярлыка из пути.
    
    
    1. Заменяем \ на /
    2. Убираем расширение
    3. Берем basename
    
    :param shortcut_path: Путь к .lnk файлу Windows
    :return: Имя ярлыка
    """
    if not shortcut_path:
        return ""
    
    # Заменяем \ на / как в bash
    normalized_path = shortcut_path.replace('\\', '/')
    
    # Убираем расширение
    name_without_ext = re.sub(r'\.[^.]*$', '', normalized_path)
    
    # Берем basename
    shortcut_name = os.path.basename(name_without_ext)
    
    logger.debug(f"Извлечение имени: '{shortcut_path}' -> '{shortcut_name}'")
    return shortcut_name


def _clean_shortcut_name(name: str) -> str:
    """
    Очистка имени ярлыка от суффиксов и нежелательных символов.
    
    :param name: Исходное имя
    :return: Очищенное имя
    """
    if not name:
        return ""
    
    # Убираем типичные суффиксы ярлыков (регистронезависимо)
    patterns_to_remove = [
        r'\s*[—–-]\s*ярлык$',           # " — ярлык", " – ярлык", " - ярлык"
        r'\s*shortcut$',                # " shortcut"  
        r'\s*\(ярлык\)$',              # " (ярлык)"
        r'\s*\(shortcut\)$',           # " (shortcut)"
        r'\s*-\s*Ярлык$',              # " - Ярлык"
        r'\s*ярлык\s*$',               # " ярлык"
        r'\.lnk$',                      # ".lnk"
        r'\.url$',                      # ".url"
    ]
    
    cleaned_name = name
    for pattern in patterns_to_remove:
        cleaned_name = re.sub(pattern, '', cleaned_name, flags=re.IGNORECASE)
    
    # Убираем лишние пробелы
    cleaned_name = cleaned_name.strip()
    
    # Ограничиваем длину до разумных пределов
    if len(cleaned_name) > 100:
        cleaned_name = cleaned_name[:100].strip()
        logger.debug(f"Имя ярлыка обрезано до 100 символов: '{cleaned_name}'")
    
    return cleaned_name


def convert_windows_path_to_linux(windows_path: str, username: str, 
                                additional_disk_mapping: Optional[Dict[str, str]] = None) -> Tuple[str, str]:
    """
    Конвертирование пути Windows в путь Linux с определением типа ссылки.
    
    1. .exe файлы - пропускаем
    2. D-Z диски - проверяем маппинг или используем NTFS_MOUNT_POINT
    3. C: пути пользователя - заменяем на /home/username
    4. http/https - оставляем как есть
    5. UNC пути (//) - добавляем smb: префикс
    6. Специальные папки (Desktop, Documents, etc.) - переименовываем
    
    :param windows_path: Исходный путь Windows
    :param username: Имя пользователя
    :param additional_disk_mapping: Словарь маппинга дисков
    :return: (link_type_prefix, converted_path)
    """
    if not windows_path:
        return "file:", ""
    
    # Нормализуем слеши
    target = windows_path.replace('\\', '/')
    link_type = "file:"
    
    # 1. Проверяем .exe файлы (пропускаем)
    if target.lower().endswith('.exe'):
        logger.info(f"Пропускаем .exe файл: {target}")
        return "", ""  # Возвращаем пустые значения для пропуска
    
    # 2. Обрабатываем диски D-Z
    if re.match(r'^[D-Zd-z]:', target):
        drive_letter = target[0].upper()
        
        # Проверяем маппинг дополнительных дисков
        if additional_disk_mapping and drive_letter in additional_disk_mapping:
            mount_point = additional_disk_mapping[drive_letter]
            target = re.sub(r'^[A-Za-z]:', mount_point, target)
        else:
            # Используем стандартную точку монтирования (аналог NTFS_MOUNT_POINT)
            ntfs_mount_point = "/media/volume"
            target = re.sub(r'^[A-Za-z]:', f"{ntfs_mount_point}/{drive_letter}", target)
    
    # 3. Обрабатываем диск C: (пути пользователя)
    elif target.startswith('C:'):
        # Заменяем C:/Users/[username]/ на /home/username/
        user_pattern = rf'C:/Users/[^/]*/'
        target = re.sub(user_pattern, f'/home/{username}/', target)
    
    # 4. HTTP/HTTPS ссылки - без префикса
    elif target.startswith(('http://', 'https://')):
        link_type = ""
    
    # 5. UNC пути (//) - добавляем smb: префикс
    elif target.startswith('//'):
        link_type = "smb:"
    
    # 6. Переименовываем специальные папки
    target = _rename_special_folders(target)
    
    logger.debug(f"Конвертация: '{windows_path}' -> '{link_type}{target}'")
    return link_type, target


def _rename_special_folders(path: str) -> str:
    """
    Переименование специальных папок согласно bash логике.
    
    :param path: Путь для обработки
    :return: Путь с переименованными папками
    """
    # Заменяем только первое вхождение каждой папки
    replacements = [
        (r'Desktop(?=/|$)', 'Desktops/Desktop1'),  # Desktop -> Desktops/Desktop1
        (r'Downloads?(?=/|$)', 'Загрузки'),        # Download/Downloads -> Загрузки
        (r'Documents(?=/|$)', 'Документы'),        # Documents -> Документы
        (r'Pictures(?=/|$)', 'Изображения'),       # Pictures -> Изображения
    ]
    
    result = path
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, count=1)
    
    return result



def sanitize_filename(name: str) -> str:
    """
    Очистка имени файла от недопустимых символов для Linux.
    
    :param name: Исходное имя файла
    :return: Безопасное имя файла
    """
    if not name or not name.strip():
        return "shortcut"
    
    # Убираем пробелы по краям
    sanitized = name.strip()
    
    # Заменяем недопустимые символы на подчеркивания
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', sanitized)
    
    # Заменяем множественные подчеркивания на одно
    sanitized = re.sub(r'_{2,}', '_', sanitized)
    
    # Убираем точки и подчеркивания в начале и конце
    sanitized = sanitized.strip('._')
    
    # Ограничиваем длину имени файла (для совместимости файловых систем)
    if len(sanitized) > 100:
        sanitized = sanitized[:100].strip('._')
    
    # Если имя стало пустым после очистки, используем fallback
    if not sanitized:
        sanitized = "shortcut"
    
    return sanitized


def escape_desktop_entry_value(value: str) -> str:
    """
    Экранирование значений для .desktop файлов.
    
    :param value: Исходное значение
    :return: Экранированное значение
    """
    # Экранируем специальные символы для .desktop формата
    return value.replace('"', '\\"').replace('\\', '\\\\')


def is_url(target: str) -> bool:
    """
    Проверка, является ли строка URL.
    
    :param target: Строка для проверки
    :return: True если это URL
    """
    try:
        parsed = urlparse(target)
        return parsed.scheme in ('http', 'https', 'ftp', 'ftps', 'file')
    except:
        return False


def _determine_file_type_and_icon(target_path: str) -> Tuple[str, str]:
    """
    Определение типа файла и подходящей иконки.
    
    :param target_path: Путь к файлу/папке
    :return: (команда_запуска, иконка)
    """
    target_lower = target_path.lower()
    
    # URL
    if target_path.startswith(('http://', 'https://', 'ftp://', 'ftps://')):
        return 'xdg-open', 'internet-web-browser'
    
    # SMB/сетевые пути
    if target_path.startswith('smb:'):
        return 'xdg-open', 'folder-remote'
    
    # Исполняемые файлы Windows
    if target_lower.endswith('.exe'):
        return 'wine', 'application-x-executable'
    
    # Папки (проверяем существование или заканчивается на /)
    is_directory = False
    if os.path.exists(target_path):
        is_directory = os.path.isdir(target_path)
    elif target_path.endswith('/'):
        is_directory = True
    
    if is_directory:
        return 'xdg-open', 'folder'
    
    # Документы
    doc_extensions = ['.doc', '.docx', '.pdf', '.txt', '.rtf', '.odt']
    if any(target_lower.endswith(ext) for ext in doc_extensions):
        return 'xdg-open', 'text-x-generic'
    
    # Изображения
    img_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg']
    if any(target_lower.endswith(ext) for ext in img_extensions):
        return 'xdg-open', 'image-x-generic'
    
    # Видео
    video_extensions = ['.mp4', '.avi', '.mkv', '.mov', '.wmv']
    if any(target_lower.endswith(ext) for ext in video_extensions):
        return 'xdg-open', 'video-x-generic'
    
    # По умолчанию
    return 'xdg-open', 'application-x-generic'

def create_shortcuts(shortcuts: List[Shortcut], user_dir: str, username: str, 
                   additional_disk_mapping: Optional[Dict[str, str]] = None) -> ShortcutsStats:
    """
    Создание .desktop файлов точно как в bash коде.
    
    Все ярлыки создаются как Type=Link с URL= (универсальный подход)
    
    :param shortcuts: Список ярлыков для создания
    :param user_dir: Директория для размещения ярлыков (Desktop)
    :param username: Имя пользователя
    :param additional_disk_mapping: Маппинг дополнительных дисков
    :return: Статистика создания ярлыков
    """
    stats = ShortcutsStats()
    
    if not shortcuts:
        logger.info("Список ярлыков пуст")
        return stats
    
    # Создаем директорию для ярлыков
    if not os.path.exists(user_dir):
        try:
            os.makedirs(user_dir, exist_ok=True)
            logger.info(f"Создана директория для ярлыков: {user_dir}")
        except Exception as e:
            logger.error(f"Не удалось создать директорию {user_dir}: {e}")
            for shortcut in shortcuts:
                stats.add_failure(shortcut.name, f"Нет доступа к директории: {e}")
            return stats

    logger.info(f"Создание {len(shortcuts)} ярлыков в {user_dir}")

    for index, shortcut in enumerate(shortcuts, 1):
        try:
            name = shortcut.name
            target = shortcut.target
            
            # Валидация имени
            if not name.strip():
                name = f"shortcut_{index}"
                stats.add_warning(name, "Пустое имя ярлыка, используется автогенерированное")
            
            # Конвертируем путь Windows в Linux
            link_type, converted_target = convert_windows_path_to_linux(target, username, additional_disk_mapping)
            
            # Пропускаем .exe файлы (как в bash)
            if not link_type and not converted_target:
                logger.info(f"Пропускаем ярлык '{name}' (.exe файл)")
                continue
            
            # Обрабатываем конфликты имен (как в bash с counter)
            safe_name = sanitize_filename(name)
            final_name = _resolve_name_conflict(user_dir, safe_name)
            
            shortcut_path = os.path.join(user_dir, f"{final_name}.desktop")
            
            # Создаем .desktop файл в стиле bash (всегда Type=Link)
            desktop_content = _create_desktop_content(final_name, link_type, converted_target)
            
            # Записываем файл
            with open(shortcut_path, 'w', encoding='utf-8') as file:
                file.write(desktop_content)
            
            # Устанавливаем права доступа
            os.chmod(shortcut_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
            
            stats.add_success(final_name)
            logger.debug(f"[{index}/{len(shortcuts)}] ✓ Ярлык создан: {final_name} -> {link_type}{converted_target}")
            
        except Exception as e:
            error_msg = str(e)
            stats.add_failure(getattr(shortcut, 'name', f'shortcut_{index}'), error_msg)
            logger.error(f"[{index}/{len(shortcuts)}] ✗ Ошибка при создании ярлыка {getattr(shortcut, 'name', '')}: {e}")

    # Итоговая статистика
    logger.info(f"Создание ярлыков завершено. {stats.summary()}")
    return stats


def _resolve_name_conflict(desktop_dir: str, base_name: str) -> str:
    """
    Разрешение конфликтов имен файлов (как counter в bash).
    
    :param desktop_dir: Директория для проверки
    :param base_name: Базовое имя файла
    :return: Уникальное имя файла
    """
    name = base_name
    counter = 1
    
    # Проверяем существование файла и добавляем счетчик если нужно
    while os.path.exists(os.path.join(desktop_dir, f"{name}.desktop")):
        name = f"{base_name}_{counter}"
        counter += 1
    
    return name


def _create_desktop_content(name: str, link_type: str, target: str) -> str:
    """
    Создание содержимого .desktop файла точно как в bash функции create_shortcut.
    
    :param name: Имя ярлыка
    :param link_type: Префикс типа ссылки (file:, smb:, или пустой)
    :param target: Целевой путь
    :return: Содержимое .desktop файла
    """
    # Экранируем имя
    escaped_name = escape_desktop_entry_value(name)
    
    # Формируем URL как в bash: $sc_type$sc_target
    url = f"{link_type}{target}"
    
    # Создаем содержимое как в bash функции create_shortcut
    content = f"""[Desktop Entry]
Name={escaped_name}
Type=Link
URL={url}
"""
    
    return content


def process_user_shortcuts(username: str, user_source_dir: str, user_desktop_dir: str, 
                         additional_disk_mapping: Optional[Dict[str, str]] = None) -> bool:
    """
    Главная функция для обработки ярлыков пользователя.
    
    Логика работы:
    1. Парсим файл links.txt из исходной директории пользователя
    2. Создаем директорию Desktop если её нет
    3. Создаем .desktop файлы на рабочем столе
    4. Возвращаем результат операции
    
    :param username: Имя пользователя
    :param user_source_dir: Исходная директория пользователя (где находится links.txt)
    :param user_desktop_dir: Директория Desktop для размещения ярлыков
    :param additional_disk_mapping: Маппинг дополнительных дисков
    :return: True если успешно, False если ошибка
    """
    logger.info(f"Начало обработки ярлыков для пользователя {username}")
    
    try:
        # 1. Парсим файл ярлыков
        shortcuts = parse_links_file(user_source_dir)
        
        if not shortcuts:
            logger.info(f"Ярлыки не найдены для пользователя {username}")
            return True  # Не ошибка, просто нет ярлыков
        
        # 2. Создаем директорию Desktop
        if not os.path.exists(user_desktop_dir):
            os.makedirs(user_desktop_dir, exist_ok=True)
            logger.info(f"Создана директория Desktop: {user_desktop_dir}")
        
        # 3. Создаем ярлыки
        stats = create_shortcuts(shortcuts, user_desktop_dir, username, additional_disk_mapping)
        
        # 4. Анализируем результаты
        total_shortcuts = len(shortcuts)
        success_rate = (stats.successful / total_shortcuts) * 100 if total_shortcuts > 0 else 0
        
        logger.info(f"Обработка ярлыков для {username} завершена:")
        logger.info(f"  Всего ярлыков: {total_shortcuts}")
        logger.info(f"  Успешно создано: {stats.successful}")
        logger.info(f"  Ошибок: {stats.failed}")
        logger.info(f"  Предупреждений: {stats.warnings}")
        logger.info(f"  Успешность: {success_rate:.1f}%")
        
        # Считаем успешным если создано хотя бы 70% ярлыков
        return success_rate >= 70.0
        
    except Exception as e:
        logger.error(f"Ошибка при обработке ярлыков для пользователя {username}: {e}")
        return False


# Функция для обратной совместимости (если используется в других местах)
def parse_links_file_legacy(user_dir: str) -> List[Tuple[str, str]]:
    """
    Функция для обратной совместимости - возвращает список кортежей.
    """
    shortcuts = parse_links_file(user_dir)
    return [(s.name, s.target) for s in shortcuts]
