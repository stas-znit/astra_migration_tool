"""
Модуль для проверки целостности данных после их копирования с использованием хеширования.
Версия с прямым вычислением хешей без использования внешней базы данных.
"""
import hashlib
import os
import logging
import shutil
import time
from datetime import datetime
import fnmatch
from src.logging.logger import setup_logger
from src.config.config_loader import load_config
from src.notify.notify import send_status

# Настройка логгера
setup_logger()
logger = logging.getLogger(__name__)
# Получение конфигурации
config = load_config()

def convert_win_path_to_linux(
    win_path: str,
    network_path: str = None,
    base_path: str = None,
    folder_mapping=None,
    desktop_rename=None,
    remove_network_path=True,
    apply_base_path=True
) -> str:
    """
    Универсальная функция для преобразования Windows-пути в «Linux-ориентированный».
    
    :param win_path: Путь в стиле Windows (или частично уже unix).
    :param network_path: Префикс сетевого пути (например, "//192.168.81.54/share/EXTNAME").
    :param base_path: Путь, который хотим подставить в качестве корневого ("/home/...").
    :param folder_mapping: Словарь вроде {"Documents": "Документы", "Downloads": "Загрузки"}.
    :param desktop_rename: Доп. словарь вроде {"Desktop": "Desktops/Desktop1"}.
    :param remove_network_path: Нужно ли убирать префикс network_path.
    :param apply_base_path: Нужно ли добавлять base_path к результату.
    :return: Строка (путь в Linux-стиле), в котором применены все преобразования.
    """
    if folder_mapping is None:
        folder_mapping = {}
    if desktop_rename is None:
        desktop_rename = {}

    # Обработка буквы диска (например, W:)
    if ":" in win_path:
        win_path = win_path[win_path.index(":")+1:]

    # Меняем '\' -> '/'
    path_unix = win_path.replace('\\', '/')

    # Если remove_network_path=True и путь начинается с network_path, удаляем этот префикс
    if remove_network_path and network_path:
        if path_unix.startswith(network_path):
            path_unix = path_unix[len(network_path):]

    # Удаляем ведущий '/', если остался
    path_unix = path_unix.lstrip('/')

    # Разбиваем на сегменты
    segments = path_unix.split('/')

    mapped_segments = []
    for seg in segments:
        if seg in desktop_rename:
            # например, "Desktop" -> "Desktops/Desktop1"
            repl = desktop_rename[seg].split('/')
            mapped_segments.extend(repl)
        else:
            # folder_mapping, e.g. "Documents"->"Документы"
            if seg in folder_mapping:
                seg = folder_mapping[seg]
            mapped_segments.append(seg)

    if apply_base_path and base_path:
        # Склеиваем с base_path => /home/temp + mapped_segments
        final_path = os.path.join(base_path, *mapped_segments)
        final_path = os.path.normpath(final_path)
    else:
        # Просто '/'.join
        final_path = '/'.join(mapped_segments)

    return final_path


def calculate_file_hash(file_path, algorithm='sha256'):
    """
    Вычисление хеша файла с использованием указанного алгоритма.

    :param file_path: Путь к файлу.
    :param algorithm: Алгоритм хеширования ('sha256', 'md5', и т.д.).
    :return: Хеш файла или None в случае ошибки.
    """
    try:
        hash_func = hashlib.new(algorithm)
    except ValueError:
        logger.error(f"Неподдерживаемый алгоритм хеширования: {algorithm}")
        return None

    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except FileNotFoundError:
        logger.error(f"Файл {file_path} не найден.")
        return None
    except PermissionError:
        logger.error(f"Недостаточно прав для доступа к файлу {file_path}.")
        return None
    except Exception as e:
        logger.error(f"Ошибка при вычислении хеша файла {file_path}: {e}")
        return None


def compare_file_sizes(source_file, target_file):
    """
    Сравнение размера файлов

    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :return: True, если размер совпадает, иначе False
    """
    try:
        source_size = os.path.getsize(source_file)
        target_size = os.path.getsize(target_file)
        return source_size == target_size
    except Exception as e:
        logger.error(f"Ошибка при сравнении размеров файлов {source_file} и {target_file}: {e}")
        return False


def compare_file_metadata(source_file, target_file):
    """
    Сравнение метаданных файла

    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :return: True, если метаданные совпадают, иначе False
    """
    try:
        source_stat = os.stat(source_file)
        target_stat = os.stat(target_file)
        # Сравниваем размер и время модификации
        return (
            source_stat.st_size == target_stat.st_size and
            int(source_stat.st_mtime) == int(target_stat.st_mtime)
        )
    except Exception as e:
        logger.error(f"Ошибка при сравнении метаданных файлов {source_file} и {target_file}: {e}")
        return False


def check_file_readability(file_path):
    """
    Проверяет возможность чтения файла

    :param file_path: Путь к файлу
    :return: True, если файл удалось прочитать, иначе False
    """
    try:
        with open(file_path, 'rb') as f:
            f.read(1024)  # Читаем первые 1КБ
        return True
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}")
        return False


def get_exclude_patterns():
    """
    Получает список шаблонов файлов и директорий для исключения.
    
    :return: (list, list) - списки исключаемых директорий и файлов
    """
    exclude_dirs = config.get("EXCLUDE_DIRS", [])
    exclude_files = config.get("EXCLUDE_FILES", [])
    return exclude_dirs, exclude_files


def should_exclude_file(file_path, exclude_patterns):
    """
    Проверяет, соответствует ли файл шаблонам исключения.
    
    :param file_path: Путь к файлу для проверки
    :param exclude_patterns: Список шаблонов для исключения
    :return: True, если файл нужно исключить
    """
    file_name = os.path.basename(file_path)
    
    # Исключаем скрытые файлы (начинающиеся с точки)
    if file_name.startswith('.'):
        return True
        
    # Проверяем соответствие шаблонам
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(file_name, pattern):
            return True
    
    return False


def check_integrity(source_dir, target_dir, discrepancies_file='/var/log/discrepancies.txt', report_data=None):
    """
    Проверка целостности файлов между исходной и целевой директориями путем прямого 
    вычисления и сравнения хешей.
    
    :param source_dir: Исходная директория
    :param target_dir: Целевая директория
    :param discrepancies_file: Файл для записи найденных несоответствий
    :param report_data: Словарь для отчета
    :return: True если все файлы прошли проверку, иначе False
    """
    discrepancies = []
    
    logger.info(f"Начало проверки целостности данных между {source_dir} и {target_dir}")
    
    # Получаем метод проверки целостности
    integrity_check_method = config.get("INTEGRITY_CHECK_METHOD", "size")
    logger.info(f"Метод проверки целостности: {integrity_check_method}")
    
    # Создаем директорию для файла несоответствий, если не существует
    if not os.path.exists(os.path.dirname(discrepancies_file)):
        os.makedirs(os.path.dirname(discrepancies_file), exist_ok=True)
    
    # Получаем списки исключений
    exclude_dirs, exclude_files = get_exclude_patterns()
    logger.info(f"Исключаемые директории: {exclude_dirs}")
    logger.info(f"Исключаемые файлы: {exclude_files}")
    
    # Сопоставления директорий
    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}
    
    # Сканируем исходную директорию и составляем список файлов для проверки
    files_to_check = []
    logger.info(f"Сканирование исходной директории: {source_dir}")
    
    try:
        for root, dirs, files in os.walk(source_dir, topdown=True):
            # Фильтруем директории исключения
            dirs[:] = [d for d in dirs if d not in exclude_dirs and 
                     not os.path.join(os.path.relpath(root, source_dir), d) in exclude_dirs]
            
            for file in files:
                source_file_path = os.path.join(root, file)
                
                # Проверяем, не подходит ли файл под шаблоны исключения
                if should_exclude_file(source_file_path, exclude_files):
                    logger.debug(f"Исключен файл: {source_file_path}")
                    continue
                
                try:
                    # Получаем относительный путь
                    relative_path = os.path.relpath(source_file_path, source_dir)
                    relative_path = relative_path.replace('\\', '/')
                    
                    # Получаем размер файла
                    file_size = os.path.getsize(source_file_path)
                    
                    files_to_check.append({
                        'relative_path': relative_path,
                        'source_file': source_file_path,
                        'file_size': file_size
                    })
                except Exception as e:
                    logger.error(f"Ошибка при сканировании файла {source_file_path}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при сканировании исходной директории: {e}")
        if report_data is not None:
            report_data['error'] = f"Ошибка при сканировании исходной директории: {e}"
        return False
    
    total_files = len(files_to_check)
    total_size = sum(item['file_size'] for item in files_to_check)
    
    logger.info(f"Найдено {total_files} файлов для проверки, общий размер: {total_size / (1024 * 1024):.2f} MB")
    
    if total_files == 0:
        logger.warning("Нет файлов для проверки целостности")
        return True
    
    # Обновляем информацию о проверке
    if report_data is not None:
        report_data['total_files'] = total_files
        report_data['total_size'] = total_size
    
    # Отправляем начальный статус
    send_status(
        progress=0,
        status="Начало проверки целостности данных",
        user=report_data.get('username') if report_data else None,
        stage="Проверка целостности",
        data_volume=f"{total_size / (1024 * 1024):.2f} MB",
        eta="Рассчитывается..."
    )
    
    # Инициализируем переменные для расчёта ETA
    verify_start_time = time.time()
    files_checked = 0
    data_checked = 0
    
    # Проверяем каждый файл
    for idx, item in enumerate(files_to_check):
        rel_path = item['relative_path']
        source_file_path = item['source_file']
        file_size = item['file_size']
        
        # Преобразуем путь для целевого файла
        converted_path = convert_win_path_to_linux(
            win_path=rel_path,
            network_path=None,
            base_path=target_dir,
            folder_mapping=folder_mapping,
            desktop_rename=desktop_rename,
            remove_network_path=False,
            apply_base_path=True
        )
        target_file_path = converted_path
        
        # Каждый 100-й файл или первые 5 - выводим детальную информацию
        detail_log = idx < 5 or idx % 100 == 0
        if detail_log:
            logger.info(f"Проверка файла [{idx+1}/{total_files}]: {rel_path}")
            logger.info(f"  Исходный путь: {source_file_path}")
            logger.info(f"  Целевой путь: {target_file_path}")
        
        # Проверяем существование целевого файла
        if not os.path.exists(target_file_path):
            error_msg = f"Файл отсутствует в целевой директории: {target_file_path}"
            logger.error(error_msg)
            discrepancies.append(f"Файл отсутствует: {target_file_path}")
            files_checked += 1
            data_checked += file_size
            continue
        
        # Выполняем проверку в зависимости от выбранного метода
        integrity_ok = False
        
        if integrity_check_method == 'hash':
            # Вычисляем хеши обоих файлов и сравниваем
            source_hash = calculate_file_hash(source_file_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
            target_hash = calculate_file_hash(target_file_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
            
            if source_hash is None or target_hash is None:
                error_msg = f"Не удалось вычислить хеш для файлов"
                logger.error(error_msg)
                discrepancies.append(f"Ошибка вычисления хеша: {target_file_path}")
            elif source_hash != target_hash:
                error_msg = f"Хеш-суммы не совпадают: {source_hash} != {target_hash}"
                if detail_log:
                    logger.error(f"  {error_msg}")
                discrepancies.append(f"Несовпадение хеш-сумм: {target_file_path}")
            else:
                integrity_ok = True
                if detail_log:
                    logger.info(f"  Хеш-суммы совпадают: {source_hash}")
        
        elif integrity_check_method == 'size':
            # Сравниваем размеры файлов
            integrity_ok = compare_file_sizes(source_file_path, target_file_path)
            if not integrity_ok:
                error_msg = f"Размеры файлов не совпадают"
                if detail_log:
                    source_size = os.path.getsize(source_file_path)
                    target_size = os.path.getsize(target_file_path)
                    logger.error(f"  {error_msg}: {source_size} != {target_size}")
                discrepancies.append(f"Несовпадение размеров файлов: {target_file_path}")
        
        elif integrity_check_method == 'metadata':
            # Сравниваем метаданные файлов
            integrity_ok = compare_file_metadata(source_file_path, target_file_path)
            if not integrity_ok:
                error_msg = f"Метаданные файлов не совпадают"
                if detail_log:
                    logger.error(f"  {error_msg}")
                discrepancies.append(f"Несовпадение метаданных файлов: {target_file_path}")
        
        else:
            logger.error(f"Неизвестный метод проверки целостности: {integrity_check_method}")
            discrepancies.append(f"Неизвестный метод проверки целостности: {integrity_check_method}")
        
        # Обновляем счетчики и информацию о проверке
        files_checked += 1
        data_checked += file_size
        
        if integrity_ok and report_data is not None:
            report_data['files_verified'] += 1
        
        # Рассчитываем прогресс и ETA
        elapsed_time = time.time() - verify_start_time
        progress = (files_checked / total_files) * 100
        
        # Обновляем статус каждые 10 файлов или для последних 5 файлов
        if idx % 10 == 0 or idx >= total_files - 5:
            if elapsed_time > 0 and data_checked > 0:
                speed = data_checked / elapsed_time  # байт/сек
                remaining_size = total_size - data_checked
                
                if speed > 0:
                    eta_seconds = remaining_size / speed
                    eta_formatted = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
                else:
                    eta_formatted = "Рассчитывается..."
            else:
                eta_formatted = "Рассчитывается..."
            
            send_status(
                progress=progress,
                status=f"Проверка целостности: {files_checked}/{total_files} файлов",
                user=report_data.get('username') if report_data else None,
                stage="Проверка целостности",
                data_volume=f"{data_checked/(1024*1024):.2f} MB / {total_size/(1024*1024):.2f} MB",
                eta=eta_formatted
            )
    
    # Сохраняем список несоответствий, если они есть
    if discrepancies:
        logger.error(f"Обнаружены несоответствия в {len(discrepancies)} файлах")
        with open(discrepancies_file, 'w', encoding='utf-8') as f:
            for item in discrepancies:
                f.write(f"{item}\n")
        
        logger.info(f"Список несоответствий сохранён в файл {discrepancies_file}")
        
        if report_data is not None:
            report_data['discrepancies'] = discrepancies
        
        # Финальный статус с ошибками
        send_status(
            progress=100,
            status=f"Проверка целостности завершена с ошибками ({len(discrepancies)} несоответствий)",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{data_checked/(1024*1024):.2f} MB",
            eta="0:00:00"
        )
        
        return False
    else:
        logger.info("Все файлы прошли проверку целостности")
        
        # Финальный статус без ошибок
        send_status(
            progress=100,
            status="Проверка целостности завершена успешно",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{data_checked/(1024*1024):.2f} MB",
            eta="0:00:00"
        )
        
        return True