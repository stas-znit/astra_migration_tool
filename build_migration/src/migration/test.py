import hashlib
import os
import glob
import logging
import shutil
import time
from datetime import datetime
from src.logging.logger import setup_logger
from src.config.config_loader import load_config

# Настройка логгера
setup_logger()
logger = logging.getLogger(__name__)
# Получение конфигурации
config = load_config()

def load_hashes_from_file(hash_file_path):
    """
    Читает хеши из файла и возвращает словарь, где ключ — путь к файлу, значение — хеш.
    """
    hashes = {}
    try:
        with open(hash_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    # Разделяем по первому двоеточию
                    hash_value, file_path = line.split(':', 1)
                    hash_value = hash_value.strip()
                    file_path = file_path.strip()
                    # Преобразуем Windows путь в Unix-путь
                    file_path = file_path.replace('\\', '/')
                    hashes[file_path] = hash_value
    except FileNotFoundError:
        logger.error(f"Файл с хешами {hash_file_path} не найден.")
    except Exception as e:
        logger.error(f"Ошибка при чтении файла с хешами {hash_file_path}: {e}")
    return hashes

def calculate_file_hash(file_path, algorithm='sha256'):
    """
    Вычисление хеша файла с использованием указанного алгоритма.
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

def find_latest_hash_file(directory, pattern):
    """
    Ищет самый свежий файл, соответствующий заданному паттерну, в указанной директории.
    """
    search_pattern = os.path.join(directory, pattern)
    files = glob.glob(search_pattern)
    if not files:
        logger.error(f"Не найдены файлы, соответствующие паттерну {search_pattern}")
        return None

    # Сортируем файлы по времени модификации в обратном порядке
    files.sort(key=os.path.getmtime, reverse=True)
    latest_file = files[0]
    logger.info(f"Найден файл с хешами: {latest_file}")
    return latest_file

def compare_file_sizes(source_file, target_file):
    """
    Сравнение размера файлов
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
    Проверка чтения файла
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
    Повторное копирование файла
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
    """
    for attempt in range(retries + 1):
        calculated_hash = calculate_file_hash(file_path, algorithm=algorithm)
        if calculated_hash == expected_hash:
            return True  # Хеш совпадает
        elif attempt < retries:
            logger.warning(f"Хеш не совпал для файла {file_path}, повторная попытка {attempt + 1}")
    return False  # Хеш не совпадает после всех попыток

def check_integrity(source_dir, target_dir, hash_directory=None, hash_file_pattern=None, discrepancies_file='/var/log/discrepancies.txt', report_data=None):
    """
    Проверка целостности файлов между исходной и целевой директориями.
    Добавлены статус-обновления, замеры времени и ETA при сборе хеш-сумм и проверке файлов.
    """
    discrepancies = []

    integrity_check_method = config.INTEGRITY_CHECK_METHOD

    folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки', 'Pictures': 'Изображения'}
    desktop_rename = {'Desktop': os.path.join('Desktops', 'Desktop1')}

    if not os.path.exists(os.path.dirname(discrepancies_file)):
        os.makedirs(os.path.dirname(discrepancies_file), exist_ok=True)

    files_to_check = []
    # Собираем список файлов с их размером
    for root, _, files in os.walk(source_dir):
        for file in files:
            source_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(source_file_path, source_dir)
            relative_path = relative_path.replace('\\', '/')
            file_size = os.path.getsize(source_file_path)
            files_to_check.append({
                'relative_path': relative_path,
                'file_size': file_size
            })

    total_files_to_check = len(files_to_check)
    files_checked = 0

    # Если используем метод 'hash', собираем хеши
    expected_hashes = {}
    if integrity_check_method == 'hash':
        # Рассчитываем общий объём данных для хеширования
        total_data_size = sum(item['file_size'] for item in files_to_check)
        hashed_data_size = 0

        if hash_directory and hash_file_pattern:
            hash_file_path = find_latest_hash_file(hash_directory, hash_file_pattern)
            if hash_file_path:
                # Загружаем ожидаемые хеши из файла
                expected_hashes = load_hashes_from_file(hash_file_path)
                if not expected_hashes:
                    logger.error("Не удалось загрузить хеши из файла. Проверка целостности невозможна.")
                    return False
            else:
                logger.info("Файл с хешами не найден. Собираем хеши исходных файлов самостоятельно.")
        else:
            logger.info("Файл с хешами не указан. Собираем хеши исходных файлов самостоятельно.")

        # Если хеши не были загружены, собираем их
        if not expected_hashes:
            total_files_to_hash = total_files_to_check
            files_hashed = 0
            hashing_start_time = time.time()

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
                file_size = item['file_size']
                source_file_path = os.path.join(source_dir, relative_path)

                start_time = time.time()
                expected_hash = calculate_file_hash(source_file_path, algorithm=config.HASH_ALGORITHM)
                end_time = time.time()

                expected_hashes[relative_path] = expected_hash
                files_hashed += 1
                hashed_data_size += file_size

                # Рассчитываем среднюю скорость хеширования
                elapsed_time = end_time - hashing_start_time
                if elapsed_time > 0:
                    average_hashing_speed = hashed_data_size / elapsed_time  # байт/сек
                else:
                    average_hashing_speed = 0

                # Оценка оставшегося времени
                remaining_data_size = total_data_size - hashed_data_size
                if average_hashing_speed > 0:
                    eta_seconds = remaining_data_size / average_hashing_speed
                    eta_formatted = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
                else:
                    eta_formatted = "Рассчитывается..."

                progress = (files_hashed / total_files_to_hash) * 100

                send_status(
                    progress=progress,
                    status=f"Сбор хешей: {files_hashed} из {total_files_to_hash} файлов",
                    user=report_data.get('username') if report_data else None,
                    stage="Сбор хеш-сумм",
                    data_volume=f"{hashed_data_size / (1024 * 1024):.2f} MB / {total_data_size / (1024 * 1024):.2f} MB",
                    eta=eta_formatted
                )

            # Отправляем финальный статус после завершения сбора хешей
            send_status(
                progress=100,
                status="Сбор хеш-сумм завершён",
                user=report_data.get('username') if report_data else None,
                stage="Сбор хеш-сумм",
                data_volume=f"{total_data_size / (1024 * 1024):.2f} MB / {total_data_size / (1024 * 1024):.2f} MB",
                eta="0:00:00"
            )

    # Отправляем начальный статус проверки целостности
    if report_data:
        target_size_mb = f"{report_data['target_size'] / (1024 * 1024):.2f} MB"
    else:
        target_size_mb = "Неизвестно"

    send_status(
        progress=0,
        status="Начата проверка целостности данных",
        user=report_data.get('username') if report_data else None,
        stage="Проверка целостности",
        data_volume=target_size_mb,
        eta="Рассчитывается..."
    )

    verify_start_time = time.time()
    verified_data_size = 0
    total_verified_data_size = sum(item['file_size'] for item in files_to_check)  # общая масса для верификации

    for item in files_to_check:
        relative_path = item['relative_path']
        file_size = item['file_size']

        # Применяем переименования директорий
        path_parts = relative_path.strip('/').split('/')
        translated_parts = []
        for part in path_parts:
            if part in desktop_rename:
                translated_parts.extend(desktop_rename[part].split(os.sep))
            else:
                translated_parts.append(folder_mapping.get(part, part))

        source_file_path = os.path.join(source_dir, relative_path)
        source_file_path = os.path.normpath(source_file_path)

        target_file_path = os.path.join(target_dir, *translated_parts)
        target_file_path = os.path.normpath(target_file_path)

        if not os.path.exists(target_file_path):
            logger.error(f"Файл {target_file_path} отсутствует в целевой директории.")
            discrepancies.append(f"Файл отсутствует: {target_file_path}")
            files_checked += 1
            verified_data_size += file_size
            # Обновляем статус верификации
            elapsed_verify_time = time.time() - verify_start_time
            if elapsed_verify_time > 0 and verified_data_size > 0:
                average_verify_speed = verified_data_size / elapsed_verify_time
                remaining_verify_data = total_verified_data_size - verified_data_size
                eta_seconds = remaining_verify_data / average_verify_speed if average_verify_speed > 0 else 0
                eta_formatted = time.strftime('%H:%M:%S', time.gmtime(eta_seconds)) if eta_seconds > 0 else "Рассчитывается..."
            else:
                eta_formatted = "Рассчитывается..."

            progress = (files_checked / total_files_to_check) * 100
            send_status(
                progress=progress,
                status=f"Проверка целостности: {files_checked} из {total_files_to_check} файлов",
                user=report_data.get('username') if report_data else None,
                stage="Проверка целостности",
                data_volume=target_size_mb,
                eta=eta_formatted
            )
            continue

        if integrity_check_method == 'hash':
            expected_hash = expected_hashes.get(relative_path)
            if expected_hash is None:
                logger.error(f"Отсутствует ожидаемый хеш для файла {source_file_path}")
                discrepancies.append(f"Отсутствует хеш: {source_file_path}")
            else:
                hash_matches = verify_hash_with_retry(
                    file_path=target_file_path,
                    expected_hash=expected_hash,
                    algorithm=config.HASH_ALGORITHM,
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
            size_matches = compare_file_sizes(source_file_path, target_file_path)
            if not size_matches:
                discrepancies.append(f"Несовпадение размеров файлов: {target_file_path}")
                logger.error(f"Размеры файлов не совпадают для файла {target_file_path}")
            else:
                logger.info(f"Файл {target_file_path} прошёл проверку размера.")
                if report_data is not None:
                    report_data['files_verified'] += 1

        elif integrity_check_method == 'metadata':
            metadata_matches = compare_file_metadata(source_file_path, target_file_path)
            if not metadata_matches:
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

        # Обновляем статус верификации с ETA
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

        progress = (files_checked / total_files_to_check) * 100
        send_status(
            progress=progress,
            status=f"Проверка целостности: {files_checked} из {total_files_to_check} файлов",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=target_size_mb,
            eta=eta_formatted
        )

    if discrepancies:
        logger.error(f"Обнаружены несоответствия в {len(discrepancies)} файлах.")
        # Записываем несоответствия в файл
        with open(discrepancies_file, 'w') as f:
            for item in discrepancies:
                f.write(f"{item}\n")
        logger.info(f"Список несоответствий сохранён в файл {discrepancies_file}")
        if report_data is not None:
            report_data['discrepancies'] = discrepancies

        # Отправляем финальный статус с ошибкой
        send_status(
            progress=100,
            status="Проверка целостности завершена с ошибками",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=target_size_mb,
            eta="0:00:00"
        )

        return False
    else:
        logger.info("Все файлы прошли проверку целостности.")
        # Отправляем финальный успешный статус
        send_status(
            progress=100,
            status="Проверка целостности завершена успешно",
            user=report_data.get('username') if report_data else None,
            stage="Проверка целостности",
            data_volume=target_size_mb,
            eta="0:00:00"
        )

        return True
