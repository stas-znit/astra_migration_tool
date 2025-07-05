"""
Модуль для проверки целостности данных после их копирования с использованием хеширования.

Функции:
    - calculate_file_hash: Вычисление хеша файла с использованием указанного алгоритма.
    - check_integrity: Проверка целостности данных между исходной и целевой директориями.
    - load_hashes_from_db: Чтение хешей из базы данных и возвращает словарь с различными вариантами путей.
    - verify_hash_with_retry: Повторное вычисление хеша с использованием повторных попыток.
    - compare_file_sizes: Сравнение размеров файлов.
    - compare_file_metadata: Сравнение метаданных файлов.
    - check_file_readability: Проверка доступности чтения файлов.
    - retry_copy_file: Повторное копирование файла.
"""
import hashlib
import os
import glob
import logging
import shutil
import time
import sqlite3
import fnmatch
from datetime import datetime
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

    Порядок действий:
      1) Заменяем обратные слэши '\\' на '/'.
      2) Если remove_network_path=True и network_path не None, 
         вырезаем префикс network_path из win_path.
      3) Разбиваем путь на сегменты (split('/')).
      4) Для каждого сегмента, если он есть в folder_mapping, заменяем его.
         Если есть в desktop_rename, тоже заменяем.
      5) Если apply_base_path=True и base_path не None, 
         итоговый путь формируем через os.path.join(base_path, *mapped_segments).
         Иначе возвращаем только список сегментов, склеенных через '/'.

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

    # 1) Меняем '\' -> '/'
    path_unix = win_path.replace('\\', '/')

    # 2) Если remove_network_path=True и path_unix.startswith(network_path),
    #    откусываем этот префикс
    if remove_network_path and network_path:
        if path_unix.startswith(network_path):
            path_unix = path_unix[len(network_path):]

    # 2.5) Удаляем ведущий '/', если остался
    path_unix = path_unix.lstrip('/')

    # 3) Разбиваем на сегменты
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


def generate_path_variants(file_path, username=None):
    """
    Генерирует различные варианты путей для поиска хешей.
    
    :param file_path: Исходный путь к файлу
    :param username: Имя пользователя (опционально)
    :return: Список возможных вариантов путей
    """
    variants = []
    
    # Нормализуем путь (заменяем обратные слеши на прямые)
    normalized_path = file_path.replace('\\', '/')
    variants.append(normalized_path)
    
    # Добавляем вариант с обратными слешами
    if '/' in normalized_path:
        variants.append(normalized_path.replace('/', '\\'))
    
    # Получаем относительные части пути
    path_parts = normalized_path.split('/')
    file_name = path_parts[-1] if path_parts else ""
    
    # Очищаем имя пользователя от доменной части, если она есть
    clean_username = username.split('@')[0] if username and '@' in username else username
    
    # Если путь содержит Desktop, добавляем специальные варианты
    if 'Desktop' in path_parts:
        try:
            desktop_index = path_parts.index('Desktop')
            # Получаем путь относительно Desktop
            rel_desktop_path = '/'.join(path_parts[desktop_index+1:])
            
            if clean_username:
                variants.append(clean_username + "/Desktop/" + rel_desktop_path)
                variants.append(clean_username + "\\Desktop\\" + rel_desktop_path.replace('/', '\\'))
            
            variants.append("Desktop/" + rel_desktop_path)
            variants.append("Desktop\\" + rel_desktop_path.replace('/', '\\'))
        except ValueError:
            pass  # Desktop не найден в пути или другая ошибка
    
    # Если указано имя пользователя, добавляем варианты с ним
    if clean_username:
        # Удаляем имя пользователя из пути, если оно уже есть в начале
        if path_parts and path_parts[0] == clean_username:
            rel_user_path = '/'.join(path_parts[1:])
        else:
            rel_user_path = normalized_path
        
        variants.append(clean_username + "/" + rel_user_path)
        variants.append(clean_username + "\\" + rel_user_path.replace('/', '\\'))
    
    # Добавляем вариант с только именем файла 
    # ВНИМАНИЕ: Этот вариант имеет самый низкий приоритет 
    # из-за высокого риска коллизий
    if file_name:
        # Добавляем имя файла только если оно уникально по некоторым критериям
        # Например, если длина больше определенного значения или содержит специфические символы
        if len(file_name) > 10 or '_' in file_name or '-' in file_name:
            variants.append(file_name)
    
    # Удаляем дубликаты, сохраняя порядок
    unique_variants = []
    for v in variants:
        if v and v not in unique_variants:
            unique_variants.append(v)
    
    return unique_variants


def load_hashes_from_db(db_path, base_path, network_path_or_username):
    """
    Читает хеши из базы данных SQLite и возвращает словарь, где ключи —
    различные варианты путей к файлам, а значение — хеш.
    
    :param db_path: Путь к файлу базы SQLite.
    :param base_path: К какому пути в Linux приклеивать результат (или пустая строка).
    :param network_path_or_username: Префикс для отрезания (e.g. //192.168.81.54/share/EXTNAME) 
                                    или имя пользователя.
    :return: dict: {"/path/to/file.txt": "hash123", "user/path/to/file.txt": "hash123", ...}
    """
    hashes = {}
    try:
        logger.info(f"Подключение к базе данных хешей: {db_path}")
        
        if not os.path.exists(db_path):
            logger.error(f"База данных не найдена: {db_path}")
            return hashes
            
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Получаем информацию о структуре таблицы
            try:
                cursor.execute("PRAGMA table_info(file_hashes)")
                columns = cursor.fetchall()
                logger.debug(f"Структура таблицы file_hashes: {columns}")
            except sqlite3.Error as e:
                logger.warning(f"Не удалось получить структуру таблицы: {e}")
            
            # Получаем количество записей
            try:
                cursor.execute("SELECT COUNT(*) FROM file_hashes")
                count = cursor.fetchone()[0]
                logger.info(f"Всего записей в таблице хешей: {count}")
            except sqlite3.Error as e:
                logger.warning(f"Не удалось получить количество записей: {e}")
                return hashes
            
            # Получаем данные о хешах
            cursor.execute("SELECT path, hash FROM file_hashes")
            rows = cursor.fetchall()
            
        logger.info(f"Загружено {len(rows)} записей с хешами из базы данных")
        
        # Обрабатываем хеши файлов, создавая различные варианты путей для каждого
        for file_path, hash_value in rows:
            if not file_path or not hash_value:
                continue
                
            # Определяем, является ли параметр именем пользователя или сетевым путем
            username = None
            if not file_path.startswith('//') and not file_path.startswith('\\\\'):
                username = network_path_or_username
                
            # Генерируем различные варианты путей для поиска
            path_variants = generate_path_variants(file_path, username)
            
            # Добавляем варианты с базовым путем, если он указан
            if base_path:
                folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
                desktop_rename = {'Desktop': 'Desktops/Desktop1'}
                
                # Преобразованный путь с применением маппингов
                converted_path = convert_win_path_to_linux(
                    win_path=file_path,
                    network_path=network_path_or_username if not username else None,
                    base_path=base_path,
                    folder_mapping=folder_mapping,
                    desktop_rename=desktop_rename,
                    remove_network_path=True,
                    apply_base_path=True
                )
                
                if converted_path:
                    path_variants.append(converted_path)
            
            # Сохраняем хеш для всех вариантов путей
            for path in path_variants:
                hashes[path] = hash_value
        
        logger.info(f"Сформировано {len(hashes)} вариантов путей с хешами")
        
        # Выводим примеры первых нескольких записей для отладки
        for i, (path, hash_val) in enumerate(list(hashes.items())[:5]):
            logger.debug(f"Пример {i+1}: Путь: {path}, Хеш: {hash_val}")
            
        return hashes
        
    except sqlite3.Error as e:
        logger.error(f"Ошибка при работе с базой данных {db_path}: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при чтении хешей из базы данных: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
    return hashes


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
    except FileNotFoundError:
        logger.error(f"Файл {file_path} не найден.")
        return None
    except PermissionError:
        logger.error(f"Недостаточно прав для доступа к файлу {file_path}.")
        return None
    except Exception as e:
        logger.error(f"Ошибка при вычислении хеша файла {file_path}: {e}")
        return None
    return hash_func.hexdigest()


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


def retry_copy_file(source_file, target_file):
    """
    Повторно копирует файл

    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :return: True, если файл удалось скопировать, иначе False
    """
    try:
        shutil.copy2(source_file, target_file)
        logger.info(f"Повторное копирование файла {source_file} выполнено.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при повторном копировании файла {source_file}: {e}")
        return False


def verify_hash_with_retry(file_path, expected_hash, algorithm, retries=1):
    """
    Повторно вычисляет хеш файла заданное количество раз, чтобы исключить временные сбои.
    Сравнение хешей выполняется без учета регистра.

    :param file_path: Путь к файлу.
    :param expected_hash: Ожидаемая хеш-сумма.
    :param algorithm: Алгоритм хеширования.
    :param retries: Количество повторов.
    :return: True, если хеш совпал, иначе False.
    """
    for attempt in range(retries + 1):
        calculated_hash = calculate_file_hash(file_path, algorithm=algorithm)
        logger.debug(f"Проверка хеша для {file_path} (попытка {attempt+1}/{retries+1}):")
        logger.debug(f"  Ожидаемый хеш: {expected_hash}")
        logger.debug(f"  Вычисленный хеш: {calculated_hash}")
        
        # Сравнение хешей без учета регистра
        if calculated_hash and expected_hash and calculated_hash.lower() == expected_hash.lower():
            logger.debug(f"  Хеши совпадают (игнорируя регистр)")
            return True  # Хеш совпадает
        elif attempt < retries:
            logger.warning(f"Хеш не совпал для файла {file_path}, повторная попытка {attempt + 1}")
            logger.warning(f"  Ожидается: {expected_hash}")
            logger.warning(f"  Получено: {calculated_hash}")
    
    logger.error(f"Хеш не совпал после {retries+1} попыток для файла {file_path}")
    return False  # Хеш не совпадает после всех попыток


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


def check_file_integrity(source_file, target_file, expected_hashes=None, file_confidence=None):
    """
    Проверяет целостность файла, используя выбранный метод проверки.
    
    :param source_file: Исходный файл
    :param target_file: Целевой файл
    :param expected_hashes: Словарь с хешами из базы данных (опционально)
    :param file_confidence: Уровень доверия при поиске по имени файла
    :return: (bool, str) - (успех проверки, сообщение об ошибке)
    """
    integrity_check_method = config.get("INTEGRITY_CHECK_METHOD", "size")
    hash_algorithm = config.get("HASH_ALGORITHM", "sha256")
    
    try:
        # Проверка хешей
        if integrity_check_method == 'hash':
            # Если предоставлены ожидаемые хеши и пути
            if expected_hashes:
                # Получаем варианты путей для поиска в словаре хешей
                source_filename = os.path.basename(source_file)
                username = os.path.basename(os.path.dirname(os.path.dirname(source_file)))
                
                path_variants = generate_path_variants(source_file, username)
                
                # Пытаемся найти хеш в словаре
                expected_hash = None
                matched_path = None
                
                for path in path_variants:
                    if path in expected_hashes:
                        expected_hash = expected_hashes[path]
                        matched_path = path
                        break
                
                # Если найден хеш по имени файла, проверяем уровень доверия
                if expected_hash and matched_path == source_filename and (file_confidence or 'low') == 'low':
                    # Дополнительная проверка: сравниваем размеры файлов
                    source_size = os.path.getsize(source_file)
                    target_size = os.path.getsize(target_file)
                    
                    if source_size != target_size:
                        logger.warning(f"Хеш найден только по имени файла, но размеры не совпадают: {source_file} ({source_size} байт) и {target_file} ({target_size} байт)")
                        return False, "Несовпадение размеров при поиске хеша по имени файла"
                
                if expected_hash:
                    # Вычисляем хеш целевого файла и сравниваем с ожидаемым
                    target_hash = calculate_file_hash(target_file, algorithm=hash_algorithm)
                    
                    if target_hash and target_hash.lower() == expected_hash.lower():
                        logger.debug(f"Хеш совпадает для файла {target_file}, найден по пути: {matched_path}")
                        return True, None
                    else:
                        error_msg = f"Несовпадение хешей: ожидается {expected_hash}, получено {target_hash}"
                        logger.error(f"{error_msg} для файла {target_file}")
                        return False, error_msg
                else:
                    logger.warning(f"Хеш не найден в базе данных для файла: {source_file}")
            
            # Если хеш не найден в базе или база не предоставлена, вычисляем хеши напрямую
            source_hash = calculate_file_hash(source_file, algorithm=hash_algorithm)
            target_hash = calculate_file_hash(target_file, algorithm=hash_algorithm)
            
            if source_hash is None or target_hash is None:
                error_msg = "Не удалось вычислить хеш одного из файлов"
                logger.error(f"{error_msg}: {source_file} или {target_file}")
                return False, error_msg
            
            if source_hash.lower() == target_hash.lower():
                return True, None
            else:
                error_msg = f"Несовпадение хешей: исходный {source_hash}, целевой {target_hash}"
                logger.error(f"{error_msg} для файла {target_file}")
                return False, error_msg
                
        # Проверка по размеру
        elif integrity_check_method == 'size':
            if compare_file_sizes(source_file, target_file):
                return True, None
            else:
                error_msg = "Несовпадение размеров файлов"
                logger.error(f"{error_msg}: {source_file} и {target_file}")
                return False, error_msg
        
        # Проверка по метаданным
        elif integrity_check_method == 'metadata':
            if compare_file_metadata(source_file, target_file):
                return True, None
            else:
                error_msg = "Несовпадение метаданных файлов"
                logger.error(f"{error_msg}: {source_file} и {target_file}")
                return False, error_msg
        
        else:
            error_msg = f"Неизвестный метод проверки целостности: {integrity_check_method}"
            logger.error(error_msg)
            return False, error_msg
    
    except Exception as e:
        error_msg = f"Ошибка при проверке целостности: {str(e)}"
        logger.error(f"{error_msg} для файлов {source_file} и {target_file}")
        return False, error_msg


def check_integrity(source_dir, target_dir, discrepancies_file='/var/log/discrepancies.txt', report_data=None):
    """
    Проверка целостности файлов путем сравнения хешей из базы данных 
    или непосредственного сравнения хешей исходных и целевых файлов.
    
    :param source_dir: Исходная директория
    :param target_dir: Целевая директория
    :param discrepancies_file: Файл для записи обнаруженных несоответствий
    :param report_data: Словарь с информацией для отчета
    :return: True если проверка успешна, иначе False
    """
    discrepancies = []
    integrity_check_method = config.get("INTEGRITY_CHECK_METHOD", "size")
    logger.info(f"Метод проверки целостности: {integrity_check_method}")
    
    # Создаем директорию для файла несоответствий
    if not os.path.exists(os.path.dirname(discrepancies_file)):
        os.makedirs(os.path.dirname(discrepancies_file), exist_ok=True)
    
    # Получаем имя пользователя из пути source_dir
    username = os.path.basename(source_dir)
    logger.info(f"Определено имя пользователя: {username}")
    
    # Загружаем хеши из базы данных, если указан соответствующий метод
    expected_hashes = {}
    if integrity_check_method == 'hash' and config.get("DATABASE_PATH"):
        db_path = config["DATABASE_PATH"]
        if os.path.exists(db_path):
            expected_hashes = load_hashes_from_db(db_path, "", username)
            
            if expected_hashes:
                logger.info(f"Успешно загружено {len(expected_hashes)} хешей из базы данных")
            else:
                logger.warning("Не удалось загрузить хеши из базы данных или она пуста")
    
    # Формируем список файлов для проверки
    files_to_check = []
    exclude_dirs, exclude_files = get_exclude_patterns()
    
    for root, dirs, files in os.walk(source_dir, topdown=True):
        # Исключаем директории
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        
        for file in files:
            # Проверяем, нужно ли исключить файл
            if should_exclude_file(file, exclude_files):
                continue
                
            # Получаем полный и относительный пути
            source_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(source_file_path, source_dir)
            
            # Определяем целевой путь (сохраняем оригинальную структуру)
            target_file_path = os.path.join(target_dir, relative_path)
            
            try:
                file_size = os.path.getsize(source_file_path)
                files_to_check.append({
                    'source_path': source_file_path,
                    'target_path': target_file_path,
                    'relative_path': relative_path,
                    'size': file_size
                })
            except Exception as e:
                logger.error(f"Ошибка при получении информации о файле {source_file_path}: {e}")
    
    # Обновляем отчет
    total_files = len(files_to_check)
    total_size = sum(item['size'] for item in files_to_check)
    
    if report_data is not None:
        report_data['total_files'] = total_files
        report_data['total_size'] = total_size
    
    logger.info(f"Найдено {total_files} файлов для проверки, общий размер: {total_size / (1024*1024):.2f} MB")
    
    # Начинаем проверку
    send_status(
        progress=0,
        status="Начало проверки целостности данных",
        user=report_data.get('username') if report_data else None,
        stage="Проверка целостности",
        data_volume=f"{total_size / (1024*1024):.2f} MB",
        eta="Рассчитывается..."
    )
    
    # Параметры для расчета прогресса
    verify_start_time = time.time()
    files_checked = 0
    verified_size = 0
    
    # Проверяем каждый файл
    for idx, item in enumerate(files_to_check):
        source_path = item['source_path']
        target_path = item['target_path']
        file_size = item['size']
        
        # Логируем для отладки (только для первых 5 файлов и каждого 100-го)
        log_detail = idx < 5 or idx % 100 == 0
        if log_detail:
            logger.debug(f"Проверка файла [{idx+1}/{total_files}]: {item['relative_path']}")
            logger.debug(f"  Исходный путь: {source_path}")
            logger.debug(f"  Целевой путь: {target_path}")
        
        # Проверяем существование целевого файла
        if not os.path.exists(target_path):
            logger.error(f"Файл не найден в целевой директории: {target_path}")
            discrepancies.append(f"Файл отсутствует: {target_path}")
            files_checked += 1
            verified_size += file_size
            continue
        
        # Выполняем проверку целостности
        integrity_ok, error_message = check_file_integrity(
            source_file=source_path, 
            target_file=target_path,
            expected_hashes=expected_hashes,
            file_confidence='low' if os.path.basename(source_path) == item['relative_path'] else 'high'
        )
        
        # Обрабатываем результат проверки
        if not integrity_ok:
            discrepancies.append(f"Несовпадение при проверке целостности: {target_path} - {error_message}")
        else:
            if report_data is not None:
                report_data['files_verified'] += 1
        
        # Обновляем счетчики
        files_checked += 1
        verified_size += file_size
        
        # Обновляем статус и прогресс
        if idx % 10 == 0 or idx >= total_files - 5:
            progress = (files_checked / total_files) * 100
            elapsed_time = time.time() - verify_start_time
            
            if elapsed_time > 0 and verified_size > 0:
                speed = verified_size / elapsed_time
                remaining_size = total_size - verified_size
                
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
                data_volume=f"{verified_size/(1024*1024):.2f} MB / {total_size/(1024*1024):.2f} MB",
                eta=eta_formatted
            )
    
    # Обрабатываем результаты проверки
    if discrepancies:
        logger.error(f"Обнаружены несоответствия в {len(discrepancies)} файлах")
        
        # Сохраняем список несоответствий в файл
        with open(discrepancies_file, 'w', encoding='utf-8') as f:
            for item in discrepancies:
                f.write(f"{item}\n")
        
        logger.info(f"Список несоответствий сохранён в {discrepancies_file}")
        
        if report_data is not None:
            report_data['discrepancies'] = discrepancies
        
        # Отправляем финальный статус с ошибками
        send_status(
            progress=100,
            status=f"Проверка целостности завершена с ошибками ({len(discrepancies)} несоответствий)",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{verified_size/(1024*1024):.2f} MB",
            eta="0:00:00"
        )
        
        return False
    else:
        logger.info("Все файлы прошли проверку целостности")
        
        # Отправляем финальный статус без ошибок
        send_status(
            progress=100,
            status="Проверка целостности завершена успешно",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{verified_size/(1024*1024):.2f} MB",
            eta="0:00:00"
        )
        
        return True
