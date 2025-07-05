"""
Модуль для проверки целостности данных после их копирования с использованием хеширования.

Функции:
    - calculate_file_hash: Вычисление хеша файла с использованием указанного алгоритма.
    - check_integrity: Проверка целостности данных между исходной и целевой директориями.
    - load_hashes_from_file: Чтение хешей из файла и возвращает словарь, где ключ — путь к файлу, значение — хеш.
    - find_latest_hash_file: Ищет самый свежий файл, соответствующий заданному паттерну, в указанной директории.
    - compare_file_sizes: Сравнение размеров файлов.
    - compare_file_metadata: Сравнение метаданных файлов.
    - check_file_readability: Проверка доступности чтения файлов.
    - retry_copy_file: Повторное копирование файла.
    - verify_hash_with_retry: Повторное вычисление хеша с использованием повторных попыток.

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
         (Порядок проверок можно контролировать, 
          или объединить их в один словарь).
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


def load_hashes_from_db(db_path, base_path, network_path):
    """
    Читает хеши из базы данных SQLite и возвращает словарь, где ключ —
    преобразованный (Windows->Linux) путь, а значение — хеш.

    :param db_path: Путь к файлу базы SQLite.
    :param base_path: К какому пути в Linux приклеивать результат.
    :param network_path: Префикс для отрезания (e.g. //192.168.81.54/share/EXTNAME).
    :return: dict: {"/home/temp/vasya/Документы/file1.txt": "abc123", ...}
    """
    hashes = {}
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT path, current_hash FROM file_hashes")
            rows = cursor.fetchall()

        # Задаём folder_mapping, desktop_rename, remove_network_path
        folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
        desktop_rename = {'Desktop': 'Desktops/Desktop1'}

        for (file_path_win, hash_value) in rows:
            converted_path = convert_win_path_to_linux(
                win_path=file_path_win,
                network_path=network_path,         # использовать для "отрезания" префикса
                base_path=base_path,               # "/home/temp"
                folder_mapping=folder_mapping,
                desktop_rename=desktop_rename,
                remove_network_path=True,
                apply_base_path=True
            )
            hashes[converted_path] = hash_value

    except sqlite3.Error as e:
        logger.error(f"Ошибка при работе с базой данных {db_path}: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при чтении хешей из базы данных: {e}")

    return hashes


def calculate_file_hash(file_path, algorithm='sha256'):
    """
    Вычисление хеша файла с использованием указанного алгоритма.

    :param file_path: Путь к файлу.
    :param algorithm: Алгоритм хеширования ('sha256', 'md5', и т.д.).
    :return: Хеш файла.
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

    :param file_path: Путь к файлу.
    :param expected_hash: Ожидаемая хеш-сумма.
    :param algorithm: Алгоритм хеширования.
    :param retries: Количество повторов.
    :return: True, если хеш совпал, иначе False.
    """
    for attempt in range(retries + 1):
        calculated_hash = calculate_file_hash(file_path, algorithm=algorithm)
        if calculated_hash == expected_hash:
            return True  # Хеш совпадает
        elif attempt < retries:
            logger.warning(f"Хеш не совпал для файла {file_path}, повторная попытка {attempt + 1}")
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



#def check_integrity(source_dir, target_dir, discrepancies_file='/var/log/discrepancies.txt', report_data=None):
    """
    Проверка целостности файлов между исходной (source_dir) и целевой (target_dir) директорией.

    Логика:
    1. Определяем метод проверки целостности (hash, size, metadata) из конфигурации.
    2. Если метод hash — пытаемся загрузить хеши из файла или вычислить их.
    3. Выполняем проверку целостности для каждого файла:
       - Если hash: сравниваем хеши.
       - Если size: сравниваем размеры файлов.
       - Если metadata: сравниваем метаданные (размер + время модификации).
    4. В случае обнаружения несоответствий записываем их в discrepancies_file.
    5. Отправляем статус в GUI через send_status, отображаем прогресс и оценочное время (ETA).

    Расчёт ETA при проверке целостности:
    - Перед началом проверки фиксируем время (verify_start_time) и считаем общий объём данных (total_verified_data_size).
    - После проверки каждого файла увеличиваем verified_data_size на размер этого файла.
    - Рассчитываем среднюю скорость (average_verify_speed = verified_data_size / elapsed_time).
    - Оцениваем оставшееся время (eta_seconds) = оставшийся объём / средняя скорость.
    - Форматируем ETA и отображаем в send_status.
    """

    discrepancies = []

    # Получаем метод проверки целостности из конфигурации
    integrity_check_method = config["INTEGRITY_CHECK_METHOD"]

    # Сопоставления директорий (Documents->Документы, и т.д.)
    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}

    # Создаём директорию для файла несоответствий, если не существует
    if not os.path.exists(os.path.dirname(discrepancies_file)):
        os.makedirs(os.path.dirname(discrepancies_file), exist_ok=True)

    # Формируем список файлов для проверки, вместе с их размерами
    files_to_check = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            source_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(source_file_path, source_dir)
            relative_path = relative_path.replace('\\', '/')
            file_size = os.path.getsize(source_file_path)
            files_to_check.append({'relative_path': relative_path, 'file_size': file_size})

    total_files_to_check = len(files_to_check)
    files_checked = 0  # Количество проверенных файлов

    # Формируем сетевой путь на основе CONNECTION и EXTNAME
    network_path = f"{config['CONNECTION']['host']}/{config['EXTNAME']}".replace('\\', '/')


    # Подготовка хешей, если используется метод 'hash'
    expected_hashes = {}
    if integrity_check_method == 'hash':
        # Если указан путь к базе данных, пытаемся загрузить хеши из неё
        if config.get("DATABASE_PATH"):  # Предполагается, что путь к БД указан в конфигурации
            expected_hashes = load_hashes_from_db(config["DATABASE_PATH"], target_dir, network_path)
            if not expected_hashes:
                logger.error("Не удалось загрузить хеши из базы данных. Собираем хеши исходных файлов самостоятельно.")
                #return False
            else:
                logger.info("Хеши успешно загружены из базы данных.")
        else:
            logger.warning("Путь к базе данных не указан в конфигурации. Собираем хеши исходных файлов самостоятельно.")
            

        # Если хеши не были загружены, вычисляем их для каждого файла
        if not expected_hashes:
            total_files_to_hash = len(files_to_check)
            files_hashed = 0

            # Отправляем начальный статус сбора хешей
            send_status(
                progress=0,
                status="Сбор хеш-сумм исходных файлов",
                user=report_data.get('username') if report_data else None,
                stage="Сбор хеш-сумм",
                data_volume="Рассчитывается...",
                eta="Рассчитывается..."
            )

            for item in files_to_check:
                relative_path = item['relative_path']
                source_file_path = os.path.join(source_dir, relative_path)
                expected_hash = calculate_file_hash(source_file_path, algorithm=config["HASH_ALGORITHM"])
                expected_hashes[relative_path] = expected_hash

                files_hashed += 1
                progress = (files_hashed / total_files_to_hash) * 100

                # Пока не рассчитываем ETA при сборе хешей, если нужно - можно аналогично добавить.
                send_status(
                    progress=progress,
                    status=f"Сбор хешей: {files_hashed} из {total_files_to_hash} файлов",
                    user=report_data.get('username') if report_data else None,
                    stage="Сбор хеш-сумм",
                    data_volume="Рассчитывается...",
                    eta="Рассчитывается..."
                )

            # Завершаем этап сбора хешей
            send_status(
                progress=100,
                status="Сбор хеш-сумм завершён",
                user=report_data.get('username') if report_data else None,
                stage="Сбор хеш-сумм",
                data_volume="Рассчитывается...",
                eta="0:00:00"
            )

    # Начинаем проверку целостности (расчёт ETA стартует отсюда)
    send_status(
        progress=0,
        status="Начата проверка целостности данных",
        user=report_data.get('username') if report_data else None,
        stage="Проверка целостности",
        data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB" if report_data else "Неизвестно",
        eta="Рассчитывается..."
    )

    # Инициализируем переменные для расчёта ETA при проверке целостности
    verify_start_time = time.time()
    verified_data_size = 0
    total_verified_data_size = sum(item['file_size'] for item in files_to_check)

    # Проходим по всем файлам и проверяем целостность
    for item in files_to_check:
        rel_path = item['relative_path']
        file_size = item['file_size']
        source_file_path = os.path.join(source_dir, rel_path)

        # *** КЛЮЧЕВОЙ МОМЕНТ *** 
        # Преобразуем relative_path так, 
        # чтобы совпадало с ключами в expected_hashes,
        # и чтобы действительно указывало на Linux-путь (с папками "Документы", ...).
        # Здесь remove_network_path=False (у нас нет префикса), 
        # base_path=target_dir => абсолютный путь. 
        converted_rel_path = convert_win_path_to_linux(
            win_path=rel_path,
            network_path=None,     
            base_path=target_dir,  # хотим абсолютный ("/home/.../Документы/...") 
            folder_mapping=folder_mapping,
            desktop_rename=desktop_rename,
            remove_network_path=False,
            apply_base_path=True
        )
        target_file_path = converted_rel_path

        if not os.path.exists(target_file_path):
            # Файл отсутствует в целевой директории - добавляем несоответствие
            logger.error(f"Файл {target_file_path} отсутствует в целевой директории.")
            discrepancies.append(f"Файл отсутствует: {target_file_path}")
            files_checked += 1
            verified_data_size += file_size
        else:
            # Проверяем целостность в зависимости от метода
            if integrity_check_method == 'hash':
                expected_hash = expected_hashes.get(relative_path)
                if expected_hash is None:
                    logger.error(f"Отсутствует ожидаемый хеш для файла {source_file_path}")
                    discrepancies.append(f"Отсутствует хеш: {source_file_path}")
                else:
                    hash_matches = verify_hash_with_retry(
                        file_path=target_file_path,
                        expected_hash=expected_hash,
                        algorithm=config["HASH_ALGORITHM"],
                        retries=1
                    )
                    if not hash_matches:
                        discrepancies.append(f"Несовпадение хеш-сумм: {target_file_path}")
                        logger.error(f"Хеш-суммы не совпадают для файла {target_file_path}")
                    else:
                        logger.info(f"Файл {target_file_path} прошёл проверку хеш-суммы.")
                        if report_data is not None:
                            report_data['files_verified'] += 1

            elif integrity_check_method == 'size':
                if not compare_file_sizes(source_file_path, target_file_path):
                    discrepancies.append(f"Несовпадение размеров файлов: {target_file_path}")
                    logger.error(f"Размеры файлов не совпадают для файла {target_file_path}")
                else:
                    logger.info(f"Файл {target_file_path} прошёл проверку размера.")
                    if report_data is not None:
                        report_data['files_verified'] += 1

            elif integrity_check_method == 'metadata':
                if not compare_file_metadata(source_file_path, target_file_path):
                    discrepancies.append(f"Несовпадение метаданных файлов: {target_file_path}")
                    logger.error(f"Метаданные файлов не совпадают для файла {target_file_path}")
                else:
                    logger.info(f"Файл {target_file_path} прошёл проверку метаданных.")
                    if report_data is not None:
                        report_data['files_verified'] += 1
            else:
                logger.error(f"Неизвестный метод проверки целостности: {integrity_check_method}")
                raise ValueError(f"Неизвестный метод проверки целостности: {integrity_check_method}")

            files_checked += 1
            verified_data_size += file_size

        # Расчёт ETA после обработки каждого файла
        elapsed_verify_time = time.time() - verify_start_time
        if elapsed_verify_time > 0 and verified_data_size > 0:
            average_verify_speed = verified_data_size / elapsed_verify_time
            remaining_verify_data = total_verified_data_size - verified_data_size
            if average_verify_speed > 0:
                eta_seconds = remaining_verify_data / average_verify_speed
                eta_formatted = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
            else:
                eta_formatted = "Рассчитывается..."
        else:
            eta_formatted = "Рассчитывается..."

        # Обновляем прогресс и отправляем статус
        progress = (files_checked / total_files_to_check) * 100
        send_status(
            progress=progress,
            status=f"Проверка целостности: {files_checked} из {total_files_to_check} файлов",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB" if report_data else "Неизвестно",
            eta=eta_formatted
        )

    # По завершении проверки анализируем, были ли несоответствия
    if discrepancies:
        logger.error(f"Обнаружены несоответствия в {len(discrepancies)} файлах.")
        with open(discrepancies_file, 'w') as f:
            for item in discrepancies:
                f.write(f"{item}\n")
        logger.info(f"Список несоответствий сохранён в файл {discrepancies_file}")
        if report_data is not None:
            report_data['discrepancies'] = discrepancies

        # Финальный статус с ошибками
        send_status(
            progress=100,
            status=f"Проверка целостности завершена с ошибками",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB" if report_data else "Неизвестно",
            eta="0:00:00"
        )

        return False
    else:
        logger.info("Все файлы прошли проверку целостности.")

        # Финальный статус без ошибок
        send_status(
            progress=100,
            status="Проверка целостности завершена успешно",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB" if report_data else "Неизвестно",
            eta="0:00:00"
        )

        return True
def check_integrity(source_dir, target_dir, discrepancies_file='/var/log/discrepancies.txt', report_data=None):
    """
    Проверка целостности файлов путем сравнения хешей из базы данных.
    Поддерживает как прямое вычисление хешей, так и сравнение с хешами из БД.
    
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
            expected_hashes = load_hashes_from_db(db_path, target_dir, username)
            
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
            
            try:
                file_size = os.path.getsize(source_file_path)
                files_to_check.append({
                    'source_path': source_file_path,
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
    
    # Маппинги для преобразования путей
    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    desktop_rename = {'Desktop': 'Desktops/Desktop1'}
    
    # Проверяем каждый файл
    for idx, item in enumerate(files_to_check):
        source_path = item['source_path']
        rel_path = item['relative_path']
        file_size = item['size']
        
        # Преобразуем путь для целевого файла
        target_path = convert_win_path_to_linux(
            win_path=rel_path,
            network_path=None,
            base_path=target_dir,
            folder_mapping=folder_mapping,
            desktop_rename=desktop_rename,
            remove_network_path=False,
            apply_base_path=True
        )
        
        # Логируем для отладки (только для первых 5 файлов и каждого 100-го)
        log_detail = idx < 5 or idx % 100 == 0
        if log_detail:
            logger.debug(f"Проверка файла [{idx+1}/{total_files}]: {rel_path}")
            logger.debug(f"  Исходный путь: {source_path}")
            logger.debug(f"  Целевой путь: {target_path}")
        
        # Проверяем существование целевого файла
        if not os.path.exists(target_path):
            logger.error(f"Файл не найден в целевой директории: {target_path}")
            discrepancies.append(f"Файл отсутствует: {target_path}")
            files_checked += 1
            verified_size += file_size
            continue
        
        # Выполняем проверку в зависимости от метода
        integrity_ok = False
        
        if integrity_check_method == 'hash':
            # Если есть хеши из базы данных, используем их
            if expected_hashes:
                # Пробуем найти хеш по различным вариантам пути
                expected_hash = None
                
                # Варианты путей для поиска в словаре хешей
                lookup_paths = [
                    target_path,                             # Преобразованный целевой путь 
                    rel_path,                                # Относительный путь 
                    rel_path.replace('\\', '/'),             # Относительный путь с прямыми слешами
                    os.path.join(username, rel_path),        # Путь с префиксом пользователя
                    os.path.join(username, rel_path).replace('\\', '/')  # То же с прямыми слешами
                ]
                
                # Пробуем найти хеш по различным путям
                for lookup in lookup_paths:
                    if lookup in expected_hashes:
                        expected_hash = expected_hashes[lookup]
                        if log_detail:
                            logger.debug(f"  Найден хеш в БД для пути: {lookup}")
                        break
                
                if expected_hash:
                    # Вычисляем хеш целевого файла и сравниваем с ожидаемым
                    target_hash = calculate_file_hash(target_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
                    
                    if target_hash == expected_hash:
                        integrity_ok = True
                        if log_detail:
                            logger.debug(f"  Хеш совпадает с ожидаемым: {target_hash}")
                    else:
                        if log_detail:
                            logger.error(f"  Хеш не совпадает: ожидается {expected_hash}, получен {target_hash}")
                        discrepancies.append(f"Несовпадение хеш-сумм: {target_path}")
                else:
                    # Хеш не найден в базе, выполняем прямое сравнение
                    if log_detail:
                        logger.warning(f"  Хеш не найден в базе, выполняем прямое сравнение")
                    
                    source_hash = calculate_file_hash(source_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
                    target_hash = calculate_file_hash(target_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
                    
                    if source_hash == target_hash:
                        integrity_ok = True
                        if log_detail:
                            logger.debug(f"  Хеши совпадают: {source_hash}")
                    else:
                        if log_detail:
                            logger.error(f"  Хеши не совпадают: {source_hash} != {target_hash}")
                        discrepancies.append(f"Несовпадение хеш-сумм: {target_path}")
            else:
                # Нет хешей из базы, выполняем прямое сравнение
                source_hash = calculate_file_hash(source_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
                target_hash = calculate_file_hash(target_path, algorithm=config.get("HASH_ALGORITHM", "sha256"))
                
                if source_hash == target_hash:
                    integrity_ok = True
                    if log_detail:
                        logger.debug(f"  Хеши совпадают: {source_hash}")
                else:
                    if log_detail:
                        logger.error(f"  Хеши не совпадают: {source_hash} != {target_hash}")
                    discrepancies.append(f"Несовпадение хеш-сумм: {target_path}")
        
        elif integrity_check_method == 'size':
            # Проверка по размеру
            if compare_file_sizes(source_path, target_path):
                integrity_ok = True
            else:
                discrepancies.append(f"Несовпадение размеров файлов: {target_path}")
        
        elif integrity_check_method == 'metadata':
            # Проверка по метаданным
            if compare_file_metadata(source_path, target_path):
                integrity_ok = True
            else:
                discrepancies.append(f"Несовпадение метаданных файлов: {target_path}")
        
        # Обновляем счетчики
        files_checked += 1
        verified_size += file_size
        
        if integrity_ok and report_data is not None:
            report_data['files_verified'] += 1
        
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