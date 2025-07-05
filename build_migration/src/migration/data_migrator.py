"""
Модуль для миграции данных из одной директории в другую с параллельным копированием файлов и проверкой целостности.

Функции:
    - shorten_filename: Сокращение длины имени файла
    - copy_file: Копирование одного файла с логированием.
    - process_special_files: Обработка специальных файлов, таких как ярлыки и конфигурации принтеров.
    - migrate_data: Основная функция для миграции данных с поддержкой исключений и фильтрации по типам файлов.
    - move_user_data: Перемещение данных пользователя из временной директории в целевую директорию.
"""

import os
import shutil
import concurrent.futures
import logging
import threading
import sys
import fnmatch
import time
from src.connection.dfs_connector import umount_dfs
from src.logging.logger import setup_logger
from src.shortcuts_printers.links_handler import parse_links_file
from src.shortcuts_printers.shortcut_creator import create_shortcuts
from src.shortcuts_printers.printer_connector import connect_printers
from src.config.config_loader import load_config
from src.notify.notify import send_status

setup_logger()
logger = logging.getLogger(__name__)
config = load_config()


# Создаём глобальный словарь для отслеживания имён файлов в каждой директории
dest_filenames = {}
dest_filenames_lock = threading.Lock()

def shorten_filename(filename, dest_dir, max_length=255):
    """
    Обрезает имя файла, если оно превышает max_length байт, сохраняя расширение.
    Добавляет индекс к имени файла для предотвращения коллизий.
    Возвращает новое имя файла.

    :param filename: имя файла
    :param dest_dir: целевая директория
    :param max_lenght: максимальная длина имени файла
    """
    # Кодируем имя файла в байты
    filename_bytes = filename.encode(sys.getfilesystemencoding())
    with dest_filenames_lock:
        if dest_dir not in dest_filenames:
            dest_filenames[dest_dir] = set()

    if len(filename_bytes) <= max_length:
        # Проверяем на коллизию
        with dest_filenames_lock:
            if filename not in dest_filenames[dest_dir]:
                dest_filenames[dest_dir].add(filename)
                return filename
    else:
        # Разделяем имя файла и расширение
        name_part, ext_part = os.path.splitext(filename)
        ext_bytes = ext_part.encode(sys.getfilesystemencoding())

        # Максимальная допустимая длина для имени файла без расширения
        max_name_bytes_length = max_length - len(ext_bytes)

        # Обрезаем имя файла до допустимой длины
        truncated_name_bytes = name_part.encode(sys.getfilesystemencoding())[:max_name_bytes_length]
        truncated_name = truncated_name_bytes.decode(sys.getfilesystemencoding(), errors='ignore')

        # Формируем новое имя файла
        new_filename = truncated_name + ext_part

    # Проверяем на коллизии и добавляем индекс
    index = 1
    base_filename = new_filename
    with dest_filenames_lock:
        while new_filename in dest_filenames[dest_dir]:
            # Добавляем индекс к имени файла
            name_part, ext_part = os.path.splitext(base_filename)
            suffix = f"_{index}"
            # Проверяем, что длина имени файла не превышает max_length
            name_bytes = name_part.encode(sys.getfilesystemencoding())
            suffix_bytes = suffix.encode(sys.getfilesystemencoding())
            ext_bytes = ext_part.encode(sys.getfilesystemencoding())

            total_length = len(name_bytes) + len(suffix_bytes) + len(ext_bytes)
            if total_length > max_length:
                # Обрезаем имя файла ещё больше
                max_name_bytes_length = max_length - len(suffix_bytes) - len(ext_bytes)
                name_bytes = name_bytes[:max_name_bytes_length]
                name_part = name_bytes.decode(sys.getfilesystemencoding(), errors='ignore')

            new_filename = f"{name_part}{suffix}{ext_part}"
            index += 1

        # Добавляем новое имя файла в набор использованных имён
        dest_filenames[dest_dir].add(new_filename)

    return new_filename

def copy_file(source_file, target_file, report_data=None, lock=None):
    """
    Копирует файл с проверкой длины имени файла и обработкой длинных имён.
    :param source_file: Файл источник
    :param target_file: Файл назначения
    :param report_data: Словарь отчет
    :param lock: Блокировка для многопоточного доступа к отчету
    """
    copied_size = 0  # Объем скопированных файлов
    try:
        target_dir = os.path.dirname(target_file)
        target_basename = os.path.basename(target_file)

        # Проверяем длину имени файла и при необходимости обрезаем
        target_basename_short = shorten_filename(target_basename, target_dir)

        # Формируем полный путь к файлу назначения с обрезанным именем
        target_file_short = os.path.join(target_dir, target_basename_short)

        # Создаем директории, если они не существуют
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        # Проверяем, нужно ли копировать файл
        if not os.path.exists(target_file_short) or os.path.getmtime(source_file) > os.path.getmtime(target_file_short):
            # Засекаем время копирования
            start_copy_time = time.time() # Время начала копирования
            shutil.copy2(source_file, target_file_short)
            end_copy_time = time.time()
            # Рассчитываем время копирования
            file_copy_time = end_copy_time - start_copy_time
            file_size = os.path.getsize(target_file_short)
            copied_size += file_size
            if report_data is not None:
                if lock:
                    with lock:
                        report_data['target_size'] += copied_size
                        report_data['files_copied'] += 1
                        report_data['total_copy_time'] += file_copy_time
        
                        # Вычисляем прогресс в процентах
                        percentage = (report_data['target_size'] / report_data['total_size']) * 100
                        # Рассчитываем ETA
                        if report_data['target_size'] > 0 and report_data['total_copy_time'] > 0:
                            avg_speed = report_data['target_size'] / report_data['total_copy_time']  # байт/сек
                            remaining_size = report_data['total_size'] - report_data['target_size']
                            remaining_time = remaining_size / avg_speed  # в секундах
                            eta = time.strftime('%H:%M:%S', time.gmtime(remaining_time))
                        else:
                            eta = "Рассчитывается..."

                        # Обновляем информацию в GUI
                        send_status(
                            progress=percentage,
                            status=f"Копирование файла {report_data['files_copied']} из {report_data['total_files']}",
                            user=report_data.get('username'),
                            stage="Копирование",
                            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB / {report_data['total_size'] / (1024 * 1024):.2f} MB",
                            eta=eta
                        )

                        # Логируем переименование файла, если оно произошло
                        if target_basename != target_basename_short:
                            report_data['renamed_files'].append({
                                'original_name': target_file,
                                'new_name': target_file_short
                            })
                else:
                    pass

            logger.debug(f'Скопирован файл {source_file} в {target_file_short}')
        else:
            logger.info(f'Пропущен файл {source_file}, целевой файл наиболее актуален')
            if report_data is not None:
                if lock:
                    with lock:
                        report_data['skipped_files'].append(source_file)
                else:
                    report_data['skipped_files'].append(source_file)
    except Exception as e:
        error_message = f'Ошибка при копировании файла {source_file} в {target_file}: {e}'
        logger.error(error_message)
        if report_data is not None:
            if lock:
                with lock:
                    report_data['copy_errors'].append(error_message)
            else:
                report_data['copy_errors'].append(error_message)

        send_status(
                    progress=0,
                    status=f"Ошибка при копировании файла {source_file}: {e}",
                    user=report_data.get('username'),
                    stage="Копирование",
                    data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB / {report_data['total_size'] / (1024 * 1024):.2f} MB",
                    eta="Неизвестно"
                )

def move_user_data(temp_user_dir, final_target_dir):
    """
    Перемещение данных пользователя из буфферной папки в целевой каталог /home
    :param temp_user_dir: Буфферная папка пользователя
    :param final_target_dir: Целевая папка в /home
    """
    # Проверяем, существует ли временная директория
    try:
        if os.path.exists(temp_user_dir):
            items = os.listdir(temp_user_dir)
            for item in items:
                s = os.path.join(temp_user_dir, item)
                d = os.path.join(final_target_dir, item)
                shutil.move(s, d)
            logger.info(f'Данные из {temp_user_dir} перемещены в {final_target_dir}.')
            # Удаляем пустую временную директорию
            os.rmdir(temp_user_dir)
        else:
            logger.error(f'Временная директория {temp_user_dir} не существует.')
    except Exception as e:
        logger.error(f'Ошибка при перемещении данных из {temp_user_dir} в {final_target_dir}: {e}')


def process_special_files(file, source_file, target_file):
    """
    Обрабатывает специальные файлы, такие как links.txt и printers.txt.

    Parameters:
    file (str): Имя файла.
    source_file (str): Путь к исходному файлу.
    target_file (str): Путь к целевому файлу.
    """
    # Проверяем, существует ли файл и его содержимое 
    try:
        if file == 'links.txt':
            user_directory = os.path.dirname(target_file)
            links = parse_links_file(source_file)
            create_shortcuts(user_directory, links)
            logger.info(f'Обработаны ярлыки для пользователя в директории {user_directory}')
        elif file == 'printers.lrs':
            connect_printers(source_file)
            logger.info('Настроены принтеры по файлу printers.txt')
    except FileNotFoundError as e:
        logger.error(f'Файл {file} не найден: {e}')
    except Exception as e:
        logger.error(f'Ошибка при обработке специального файла {file}: {e}')


def migrate_data(source_dir, target_dir, exclude_dirs=None, include_files=None, exclude_files=None, username=None, report_data=None):
    """
    Мигрирует файлы и директории из source_dir в target_dir, исключая указанные директории и файлы.
    Файлы копируются в порядке свежести (сначала самые новые).

    Parameters:
    source_dir (str): Путь к исходной директории.
    target_dir (str): Путь к целевой директории.
    exclude_dirs (list, optional): Список директорий для исключения (относительно source_dir).
    include_files (list, optional): Список расширений файлов для копирования. Если None, копируются все файлы.
    exclude_file (list, optional): Список файлов/расширений для исключения. Если None, копируются все файлы.
    username (str): Имя пользователя для формирования путей к конфигурационным файлам браузеров.
    report_data (dict, optional): Словарь для формирования отчета
    """
    # Инициализация exclude_dirs, include_files и exclude_files
    exclude_dirs = exclude_dirs or []
    include_files = include_files or []
    exclude_files = exclude_files or []
    # Инициализация folder_mapping
    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    # Инициализация desktop_rename
    desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}

    # Инициализация report_data, если она не передана
    if report_data is None:
        report_data = {}

    # Преобразуем пути исключаемых директорий в относительные
    exclude_dirs = [os.path.normpath(os.path.relpath(os.path.join(source_dir, excl), source_dir)) for excl in exclude_dirs]
    # Инициализируем общий объем данных для миграции
    total_size = 0
    # Проверяем, существует ли целевая директория
    try:
        os.makedirs(target_dir, exist_ok=True)

        # Собираем список всех файлов с их временем модификации
        files_to_copy = []
        for root, dirs, files in os.walk(source_dir, topdown=True):
            # Относительный путь от исходной директории
            rel_path = os.path.normpath(os.path.relpath(root, source_dir))

            # Исключение директорий
            dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(rel_path, d)) not in exclude_dirs]

            # Применение переименования директорий
            path_parts = rel_path.split(os.sep)
            translated_parts = []

            # Переименовываем директории
            for part in path_parts:
                if part == '.':
                    continue
                if part in desktop_rename:
                    translated_parts.extend(desktop_rename[part].split(os.sep))
                else:
                    translated_parts.append(folder_mapping.get(part, part))

            # Обработка для 'Desktop'
            if 'Desktop' in path_parts:
                dest_dir = os.path.join(target_dir, *translated_parts)
            # Перенос 'BrowserData'
            elif 'BrowserData' in path_parts:
                browser_index = path_parts.index('BrowserData')
                browser_subpath = path_parts[browser_index + 1:]
                if 'chrome' in browser_subpath:
                    dest_dir = os.path.join("/home", username, ".config/google-chrome/Default", *browser_subpath[browser_subpath.index('chrome') + 1:])
                elif 'yandex' in browser_subpath:
                    dest_dir = os.path.join("/home", username, ".config/yandex-browser/Default", *browser_subpath[browser_subpath.index('yandex') + 1:])
                else:
                    dest_dir = os.path.join(target_dir, *translated_parts)
            else:
                dest_dir = os.path.join(target_dir, *translated_parts)

            for file in files:
                # Получаем полный путь к файлу
                source_file = os.path.join(root, file)
                # Проверяем, нужно ли исключить файл
                if file.startswith('.'):
                    logger.info(f"Скрытый файл {file} исключён из копирования.")
                    source_file = os.path.join(root, file)
                    if report_data is not None:
                        report_data['skipped_files'].append(source_file)
                    continue
                exclude_file = False
                # Проверяем, нужно ли исключить файл по шаблону
                if exclude_files:
                    for pattern in exclude_files:
                        if fnmatch.fnmatch(file, pattern):
                            exclude_file = True
                            logger.info(f"Файл {file} исключён из копирования по шаблону {pattern}.")
                            report_data['skipped_files'].append(source_file)
                            break
                if exclude_file:
                    continue

                # Проверяем, нужно ли копировать файл
                if not include_files or file.endswith(tuple(include_files)):
                    source_file = os.path.join(root, file)
                    # Получаем размер файла
                    try:
                        total_size += os.path.getsize(source_file)
                    except Exception as e:
                        logger.error(f"Ошибка при получении размера файла {source_file}: {e}")
                        continue
                    # Получаем путь к целевому файлу
                    dest_file = os.path.join(dest_dir, file)
                    # Получаем время последней модификации файла
                    mtime = os.path.getmtime(source_file)
                    # Добавляем файл в список
                    files_to_copy.append((mtime, source_file, dest_file))

        # Сортируем файлы по времени модификации (самые новые первые)
        files_to_copy.sort(reverse=True, key=lambda x: x[0])

        # Определяем количество файлов для копирования
        total_files = len(files_to_copy)
        if report_data is not None:
            report_data['total_size'] = total_size
            report_data['total_files'] = total_files
        if total_files == 0:
            logger.warning("Нет файлов для копирования.")
            send_status(
                progress=100,
                status="Нет файлов для копирования",
                user=username,
                stage="Завершение",
                data_volume="0 MB",
                eta="0:00:00"
            )
            return
        
        # Отправляем начальный статус миграции
        send_status(
            progress=0,
            status="Начало миграции",
            user=username,
            stage="Подготовка",
            data_volume=f"{total_size / (1024 * 1024):.2f} MB",
            eta="Рассчитывается..."
        )


        lock = threading.Lock()  # Замок для синхронизации доступа к report_data

        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 2) as executor:
            futures = []
            for mtime, source_file, dest_file in files_to_copy:
                future = executor.submit(copy_file, source_file, dest_file, report_data, lock)
                futures.append(future)

            # Ожидание завершения копирования и обработка ошибок
            for future in concurrent.futures.as_completed(futures):
                exception = future.exception()
                if exception is not None:
                    logger.error(f'Ошибка при копировании: {exception}')

        # После завершения копирования отправляем статус
        send_status(
            progress=100,
            status="Копирование завершено",
            user=username,
            stage="Копирование",
            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB",
            eta="0:00:00"
        )

    except Exception as e:
        logger.error(f'Ошибка в процессе миграции: {e}')
        send_status(
            progress=0,
            status=f"Ошибка в процессе миграции: {e}",
            user=username,
            stage="Ошибка",
            data_volume="Неизвестно",
            eta="Неизвестно"
        )
