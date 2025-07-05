"""
Модуль для прямой миграции данных из исходной директории в целевую
с параллельным копированием файлов и проверкой целостности.
Исключает использование буферной директории.
"""

import os
import shutil
import concurrent.futures
import logging
import threading
import sys
import fnmatch
import time
import hashlib
import sqlite3
from collections import defaultdict

from src.config.config_loader import load_config
from src.migration.data_migrator import shorten_filename
from src.migration.integrity_checker import (
    calculate_file_hash, 
    compare_file_sizes, 
    compare_file_metadata,
    convert_win_path_to_linux,
    load_hashes_from_db,
    verify_hash_with_retry
)
from src.notify.notify import send_status

logger = logging.getLogger(__name__)
config = load_config()

# Создаём глобальный словарь для отслеживания имён файлов в каждой директории
dest_filenames = {}
dest_filenames_lock = threading.Lock()

# Словарь для отслеживания состояния миграции каждого пользователя
migration_state = defaultdict(dict)
migration_state_lock = threading.Lock()

# Предварительно загруженные хеши из базы данных
preloaded_hashes = {}

def direct_copy_file(source_file, target_file, source_dir, target_dir, username=None, report_data=None, lock=None):
    """
    Копирует файл напрямую в целевую директорию и выполняет проверку целостности.
    
    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :param source_dir: Корневая исходная директория
    :param target_dir: Корневая целевая директория
    :param username: Имя пользователя
    :param report_data: Словарь отчета
    :param lock: Блокировка для многопоточного доступа к отчету
    :return: (bool, str) - (успех копирования, сообщение об ошибке)
    """
    copied_size = 0
    error_message = None
    
    try:
        target_dir_path = os.path.dirname(target_file)
        target_basename = os.path.basename(target_file)
        
        # Проверяем длину имени файла и при необходимости обрезаем
        target_basename_short = shorten_filename(target_basename, target_dir_path)
        target_file_short = os.path.join(target_dir_path, target_basename_short)
        
        # Создаем директории, если они не существуют
        if not os.path.exists(target_dir_path):
            os.makedirs(target_dir_path, exist_ok=True)
        
        # Проверяем, нужно ли копировать файл
        if not os.path.exists(target_file_short) or os.path.getmtime(source_file) > os.path.getmtime(target_file_short):
            # Засекаем время копирования
            start_copy_time = time.time()
            shutil.copy2(source_file, target_file_short)
            end_copy_time = time.time()
            
            file_copy_time = end_copy_time - start_copy_time
            file_size = os.path.getsize(target_file_short)
            copied_size += file_size
            
            # Проверка целостности с использованием выбранного метода
            integrity_ok = verify_file_integrity(source_file, target_file_short, source_dir, target_dir, username)
            
            if not integrity_ok:
                error_message = f"Ошибка целостности файла: {target_file_short}"
                logger.error(error_message)
                if report_data is not None and lock:
                    with lock:
                        report_data['discrepancies'].append(f"Несовпадение целостности: {target_file_short}")
                return False, error_message
            
            # Обновляем данные отчета
            if report_data is not None and lock:
                with lock:
                    report_data['target_size'] += copied_size
                    report_data['files_copied'] += 1
                    report_data['total_copy_time'] += file_copy_time
                    report_data['files_verified'] += 1
                    
                    # Логируем переименование файла, если оно произошло
                    if target_basename != target_basename_short:
                        report_data['renamed_files'].append({
                            'original_name': target_file,
                            'new_name': target_file_short
                        })
                    
                    # Расчет прогресса и ETA
                    if report_data['total_size'] > 0:
                        percentage = min(100, (report_data['files_copied'] / report_data['total_files']) * 100)
                        
                        if report_data['target_size'] > 0 and report_data['total_copy_time'] > 0:
                            avg_speed = report_data['target_size'] / report_data['total_copy_time']
                            remaining_size = report_data['total_size'] - report_data['target_size']
                            remaining_time = remaining_size / avg_speed if avg_speed > 0 else 0
                            eta = time.strftime('%H:%M:%S', time.gmtime(remaining_time))
                        else:
                            eta = "Рассчитывается..."
                        
                        # Отправляем информацию о прогрессе
                        send_status(
                            progress=percentage,
                            status=f"Копирование и проверка: {report_data['files_copied']}/{report_data['total_files']}",
                            user=username,
                            stage="Копирование и проверка",
                            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB / {report_data['total_size'] / (1024 * 1024):.2f} MB",
                            eta=eta
                        )
            
            # Сохраняем информацию о скопированном файле для возможности восстановления
            with migration_state_lock:
                migration_state[username][source_file] = {
                    'target_file': target_file_short,
                    'size': file_size,
                    'timestamp': time.time(),
                    'verified': True
                }
            
            logger.info(f'Файл успешно скопирован и проверен: {source_file} -> {target_file_short}')
            return True, None
            
        else:
            logger.info(f'Пропущен файл {source_file}, целевой файл наиболее актуален')
            if report_data is not None and lock:
                with lock:
                    report_data['skipped_files'].append(source_file)
            return True, None
            
    except Exception as e:
        error_message = f'Ошибка при копировании файла {source_file} в {target_file}: {e}'
        logger.error(error_message)
        
        if report_data is not None and lock:
            with lock:
                report_data['copy_errors'].append(error_message)
        
        send_status(
            progress=0,
            status=f"Ошибка: {error_message}",
            user=username,
            stage="Ошибка",
            data_volume="Неизвестно",
            eta="Неизвестно"
        )
        
        return False, error_message


def verify_file_integrity(source_file, target_file, source_dir, target_dir, username=None):
    """
    Проверяет целостность скопированного файла в зависимости от метода проверки.
    
    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :param source_dir: Корневая исходная директория
    :param target_dir: Корневая целевая директория
    :param username: Имя пользователя
    :return: True если целостность подтверждена, False в противном случае
    """
    integrity_check_method = config["INTEGRITY_CHECK_METHOD"]
    
    try:
        if integrity_check_method == 'hash':
            # Проверяем, есть ли предзагруженные хеши
            if preloaded_hashes:
                # Получаем относительный путь от исходной директории
                rel_path = os.path.relpath(source_file, source_dir)
                rel_path = rel_path.replace('\\', '/')
                
                # Преобразуем путь в нужный формат
                folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
                desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}
                
                # Получаем абсолютный путь к файлу в Linux-формате
                converted_path = convert_win_path_to_linux(
                    win_path=rel_path,
                    network_path=None,
                    base_path=target_dir,
                    folder_mapping=folder_mapping,
                    desktop_rename=desktop_rename,
                    remove_network_path=False,
                    apply_base_path=True
                )
                
                # Ищем хеш файла в предзагруженном словаре
                expected_hash = preloaded_hashes.get(converted_path)
                
                if expected_hash:
                    # Вычисляем хеш целевого файла и сравниваем с ожидаемым
                    return verify_hash_with_retry(
                        file_path=target_file,
                        expected_hash=expected_hash,
                        algorithm=config["HASH_ALGORITHM"],
                        retries=config.get("RETRIES_HASH_MATCH", 1)
                    )
            
            # Если нет предзагруженных хешей или хеш не найден, вычисляем хеши напрямую
            source_hash = calculate_file_hash(source_file, algorithm=config["HASH_ALGORITHM"])
            target_hash = calculate_file_hash(target_file, algorithm=config["HASH_ALGORITHM"])
            
            if source_hash is None or target_hash is None:
                logger.error(f"Не удалось вычислить хеш для файлов {source_file} или {target_file}")
                return False
                
            return source_hash == target_hash
            
        elif integrity_check_method == 'size':
            return compare_file_sizes(source_file, target_file)
            
        elif integrity_check_method == 'metadata':
            return compare_file_metadata(source_file, target_file)
            
        else:
            logger.error(f"Неизвестный метод проверки целостности: {integrity_check_method}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при проверке целостности файла {target_file}: {e}")
        return False


def direct_migrate(source_dir, target_dir, exclude_dirs=None, exclude_files=None, username=None, report_data=None):
    """
    Выполняет прямую миграцию данных из source_dir в target_dir с одновременной проверкой целостности.
    
    :param source_dir: Исходная директория
    :param target_dir: Целевая директория (финальная, без буфера)
    :param exclude_dirs: Список директорий для исключения
    :param exclude_files: Список файлов для исключения
    :param username: Имя пользователя
    :param report_data: Словарь для отчета
    :return: True если миграция успешна, иначе False
    """
    global preloaded_hashes
    
    # Инициализация параметров
    exclude_dirs = exclude_dirs or []
    exclude_files = exclude_files or []
    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}
    
    if report_data is None:
        report_data = {
            'username': username,
            'source_dir': source_dir,
            'target_dir': target_dir,
            'total_files': 0,
            'total_size': 0,
            'target_size': 0,
            'files_copied': 0,
            'copy_errors': [],
            'renamed_files': [],
            'skipped_files': [],
            'files_verified': 0,
            'discrepancies': [],
            'total_copy_time': 0,
            'average_speed': None,
            'start_time': time.time(),
            'end_time': None
        }
    
    # Предварительно загружаем хеши из базы данных, если метод проверки 'hash'
    if config["INTEGRITY_CHECK_METHOD"] == 'hash' and config.get("DATABASE_PATH"):
        logger.info("Загрузка хешей из базы данных...")
        send_status(
            progress=0,
            status="Загрузка хешей из базы данных",
            user=username,
            stage="Подготовка",
            data_volume="Не определено",
            eta="Рассчитывается..."
        )
        
        # Формируем сетевой путь для загрузки хешей
        network_path = f"{config['CONNECTION']['host']}/{config['EXTNAME']}".replace('\\', '/')
        
        try:
            # Загружаем хеши из базы данных
            preloaded_hashes = load_hashes_from_db(config["DATABASE_PATH"], target_dir, network_path)
            
            if preloaded_hashes:
                logger.info(f"Загружено {len(preloaded_hashes)} хешей из базы данных")
            else:
                logger.warning("Не удалось загрузить хеши из базы данных, будет выполнено прямое сравнение")
        except Exception as e:
            logger.error(f"Ошибка при загрузке хешей из базы данных: {e}")
            preloaded_hashes = {}
    
    # Преобразуем пути исключаемых директорий в относительные
    exclude_dirs = [os.path.normpath(os.path.relpath(os.path.join(source_dir, excl), source_dir)) for excl in exclude_dirs]
    
    # Создаем целевую директорию
    try:
        os.makedirs(target_dir, exist_ok=True)
        
        # Собираем список файлов для копирования
        files_to_copy = []
        total_size = 0
        
        # Отправляем статус сканирования
        send_status(
            progress=0,
            status="Сканирование исходной директории",
            user=username,
            stage="Сканирование",
            data_volume="Не определено",
            eta="Рассчитывается..."
        )
        
        for root, dirs, files in os.walk(source_dir, topdown=True):
            # Относительный путь от исходной директории
            rel_path = os.path.normpath(os.path.relpath(root, source_dir))
            
            # Исключение директорий
            dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(rel_path, d)) not in exclude_dirs]
            
            # Обработка пути для целевой директории
            path_parts = rel_path.split(os.sep)
            translated_parts = []
            
            for part in path_parts:
                if part == '.':
                    continue
                if part in desktop_rename:
                    translated_parts.extend(desktop_rename[part].split(os.sep))
                else:
                    translated_parts.append(folder_mapping.get(part, part))
            
            # Обработка специальных путей
            if 'Desktop' in path_parts:
                dest_dir = os.path.join(target_dir, *translated_parts)
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
            
            # Обработка файлов
            for file in files:
                # Пропускаем скрытые файлы
                if file.startswith('.'):
                    logger.info(f"Скрытый файл {file} исключён из копирования.")
                    if report_data is not None:
                        report_data['skipped_files'].append(os.path.join(root, file))
                    continue
                
                # Проверяем исключения по шаблону
                exclude_file = False
                if exclude_files:
                    for pattern in exclude_files:
                        if fnmatch.fnmatch(file, pattern):
                            exclude_file = True
                            logger.info(f"Файл {file} исключён из копирования по шаблону {pattern}.")
                            if report_data is not None:
                                report_data['skipped_files'].append(os.path.join(root, file))
                            break
                
                if exclude_file:
                    continue
                
                # Добавляем файл в список для копирования
                source_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)
                
                try:
                    file_size = os.path.getsize(source_file)
                    total_size += file_size
                    
                    # Время модификации для сортировки
                    mtime = os.path.getmtime(source_file)
                    
                    files_to_copy.append((mtime, source_file, dest_file, file_size))
                except Exception as e:
                    logger.error(f"Ошибка при получении информации о файле {source_file}: {e}")
                    continue
        
        # Сортируем файлы по времени модификации (самые новые первые)
        files_to_copy.sort(reverse=True, key=lambda x: x[0])
        
        # Обновляем отчет
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
            return True
        
        # Отправляем начальный статус
        send_status(
            progress=0,
            status="Начало прямой миграции с проверкой целостности",
            user=username,
            stage="Подготовка",
            data_volume=f"{total_size / (1024 * 1024):.2f} MB",
            eta="Рассчитывается..."
        )
        
        # Многопоточное копирование с проверкой целостности
        lock = threading.Lock()
        success = True
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 2) as executor:
            futures = []
            
            for _, source_file, dest_file, _ in files_to_copy:
                future = executor.submit(
                    direct_copy_file, 
                    source_file, 
                    dest_file, 
                    source_dir, 
                    target_dir,
                    username,
                    report_data, 
                    lock
                )
                futures.append(future)
            
            # Ожидание завершения и обработка результатов
            for future in concurrent.futures.as_completed(futures):
                try:
                    result, error = future.result()
                    if not result:
                        success = False
                except Exception as e:
                    logger.error(f"Ошибка при обработке результата: {e}")
                    success = False
        
        # Сохраняем состояние миграции в файл
        save_migration_state(username)
        
        # Устанавливаем время окончания
        if report_data is not None:
            report_data['end_time'] = time.time()
        
        # Итоговый статус миграции
        if success:
            send_status(
                progress=100,
                status="Миграция с проверкой целостности успешно завершена",
                user=username,
                stage="Завершение",
                data_volume=f"{report_data.get('target_size', 0) / (1024 * 1024):.2f} MB",
                eta="0:00:00"
            )
            logger.info(f"Миграция с проверкой целостности для пользователя {username} успешно завершена")
            return True
        else:
            # Формируем список файлов с ошибками
            discrepancies_file = config["HASH_MISMATCH_FILE"]
            if report_data and report_data.get('discrepancies'):
                os.makedirs(os.path.dirname(discrepancies_file), exist_ok=True)
                with open(discrepancies_file, 'w') as f:
                    for item in report_data['discrepancies']:
                        f.write(f"{item}\n")
            
            send_status(
                progress=100,
                status="Миграция с проверкой целостности завершена с ошибками",
                user=username,
                stage="Завершение с ошибками",
                data_volume=f"{report_data.get('target_size', 0) / (1024 * 1024):.2f} MB",
                eta="0:00:00"
            )
            logger.warning(f"Миграция с проверкой целостности для пользователя {username} завершена с ошибками")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при выполнении прямой миграции: {e}")
        send_status(
            progress=0,
            status=f"Ошибка при выполнении прямой миграции: {e}",
            user=username,
            stage="Ошибка",
            data_volume="Неизвестно",
            eta="Неизвестно"
        )
        return False


def save_migration_state(username):
    """
    Сохраняет состояние миграции в файл.
    
    :param username: Имя пользователя
    """
    try:
        # Формируем путь к файлу состояния
        state_dir = os.path.dirname(config.get("STATE_FILE", "/var/lib/migration_state"))
        os.makedirs(state_dir, exist_ok=True)
        
        state_file = os.path.join(state_dir, f"migration_state_{username}.json")
        
        import json
        with open(state_file, 'w') as f:
            json.dump(migration_state.get(username, {}), f, indent=2)
            
        logger.info(f"Состояние миграции пользователя {username} сохранено в {state_file}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении состояния миграции: {e}")


def load_migration_state(username):
    """
    Загружает состояние миграции из файла.
    
    :param username: Имя пользователя
    :return: Словарь с состоянием миграции
    """
    try:
        state_dir = os.path.dirname(config.get("STATE_FILE", "/var/lib/migration_state"))
        state_file = os.path.join(state_dir, f"migration_state_{username}.json")
        
        if not os.path.exists(state_file):
            logger.info(f"Файл состояния миграции {state_file} не найден")
            return {}
        
        import json
        with open(state_file, 'r') as f:
            state = json.load(f)
            
        logger.info(f"Состояние миграции пользователя {username} загружено из {state_file}")
        return state
    except Exception as e:
        logger.error(f"Ошибка при загрузке состояния миграции: {e}")
        return {}


def resume_direct_migration(source_dir, target_dir, username, report_data=None):
    """
    Возобновляет прерванную прямую миграцию данных пользователя.
    
    :param source_dir: Исходная директория
    :param target_dir: Целевая директория
    :param username: Имя пользователя
    :param report_data: Словарь для отчета
    :return: True если миграция успешно возобновлена, иначе False
    """
    global migration_state
    
    logger.info(f"Возобновление прерванной миграции для пользователя {username}")
    
    # Загружаем состояние миграции
    user_state = load_migration_state(username)
    
    if not user_state:
        logger.info(f"Состояние миграции для пользователя {username} не найдено, выполняем полную миграцию")
        return direct_migrate(source_dir, target_dir, exclude_dirs=config["EXCLUDE_DIRS"], 
                              exclude_files=config["EXCLUDE_FILES"], username=username, report_data=report_data)
    
    # Обновляем глобальное состояние
    with migration_state_lock:
        migration_state[username] = user_state
    
    logger.info(f"Найдена информация о {len(user_state)} ранее скопированных файлах")
    
    # Инициализируем данные отчета, если необходимо
    if report_data is None:
        report_data = {
            'username': username,
            'source_dir': source_dir,
            'target_dir': target_dir,
            'total_files': 0,
            'total_size': 0,
            'target_size': 0,
            'files_copied': 0,
            'copy_errors': [],
            'renamed_files': [],
            'skipped_files': [],
            'files_verified': 0,
            'discrepancies': [],
            'total_copy_time': 0,
            'average_speed': None,
            'start_time': time.time(),
            'end_time': None
        }
    
    # Подсчитываем уже скопированные файлы для отчета
    for source_path, info in user_state.items():
        if info.get('verified', False):
            report_data['files_copied'] += 1
            report_data['files_verified'] += 1
            report_data['target_size'] += info.get('size', 0)
    
    # Находим файлы, которые нужно добавить к миграции
    return direct_migrate(source_dir, target_dir, exclude_dirs=config["EXCLUDE_DIRS"], 
                          exclude_files=config["EXCLUDE_FILES"], username=username, report_data=report_data)