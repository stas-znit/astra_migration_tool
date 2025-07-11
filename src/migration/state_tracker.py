"""
Модуль для управления состоянием миграции с двойным сохранением.

Логика:
- Храним всё в одном JSON-файле (state_file) на сетевом хранилище.
- ДОПОЛНИТЕЛЬНО: дублируем состояние локально для управляющего сервиса.
- Структура JSON:
  {
    "global": {
      "status": "in_progress",
      "last_update": "...",
      "last_heartbeat": "...",
      "overall_progress": 45.5,
      "current_user": "username",
      "total_users": 10,
      "users_completed": 4,
      ...
    },
    "users": {
      "vasya": "success",
      "petya": "failed",
      ...
    }
  }

Функции:
    load_state() -> dict
    save_full_state(state_dict)
    save_state_dual(state_dict)  # двойное сохранение
    update_global_state(**kwargs)
    update_user_state(user, status)
    get_local_state_for_service() -> dict  # для управляющего сервиса
"""

"""
Модуль для управления состоянием миграции с защитой от блокировок.

Исправления:
1. Добавлена файловая блокировка для предотвращения конфликтов
2. Таймауты для операций чтения/записи
3. Разделение файлов для чтения и записи
4. Обработка ситуаций блокировки файлов
"""

import json
import os
import logging
import tempfile
import shutil
import datetime
import fcntl
import time
import errno
from pathlib import Path
from contextlib import contextmanager
from src.logging.logger import setup_logger
from src.config.config_loader import load_config
from src.errors.error_codes import ErrorHandler, MigrationErrorCodes, create_error_handler

error_handler = None

setup_logger()
logger = logging.getLogger(__name__)

# Загружаем конфиг
config = load_config()
network_state_file = config["STATE_FILE"]

# Локальные файлы состояния для управляющего сервиса
LOCAL_STATE_FILE = "/tmp/migration_state.json"
SERVICE_STATE_FILE = "/var/lib/migration-service/state.json"
SERVICE_MINIMAL_FILE = "/var/lib/migration-service/current_state.json"

# НОВОЕ: Файл блокировки для синхронизации
LOCK_FILE = "/var/lib/migration-service/state.lock"

# НОВОЕ: Разделение файлов для чтения супервизором
SUPERVISOR_READ_FILE = "/var/lib/migration-service/supervisor_state.json"

# Таймауты для операций с файлами
LOCK_TIMEOUT = 5.0  # 5 секунд на получение блокировки
READ_TIMEOUT = 3.0  # 3 секунды на чтение
WRITE_TIMEOUT = 10.0  # 10 секунд на запись

@contextmanager
def file_lock(lock_file_path, timeout=LOCK_TIMEOUT, mode='w'):
    """
    Контекстный менеджер для файловой блокировки с таймаутом.
    """
    lock_fd = None
    try:
        # Создаем директорию для lock файла если её нет
        os.makedirs(os.path.dirname(lock_file_path), exist_ok=True)
        
        # Открываем файл блокировки
        lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        
        # Пытаемся получить блокировку с таймаутом
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug(f"Блокировка получена: {lock_file_path}")
                yield lock_fd
                return
            except IOError as e:
                if e.errno == errno.EAGAIN or e.errno == errno.EACCES:
                    time.sleep(0.1)  # Ждем 100мс и пробуем снова
                    continue
                else:
                    raise
        
        # Таймаут истек
        raise TimeoutError(f"Не удалось получить блокировку {lock_file_path} за {timeout}с")
        
    except Exception as e:
        logger.warning(f"Ошибка работы с блокировкой {lock_file_path}: {e}")
        # Если блокировка не удалась, продолжаем без неё (degraded mode)
        yield None
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
                logger.debug(f"Блокировка освобождена: {lock_file_path}")
            except:
                pass

def safe_read_json(file_path, timeout=READ_TIMEOUT):
    """
    Безопасное чтение JSON файла с таймаутом.
    """
    if not os.path.exists(file_path):
        return None
        
    start_time = time.time()
    last_error = None
    
    while time.time() - start_time < timeout:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (IOError, OSError) as e:
            if e.errno == errno.EACCES or "resource temporarily unavailable" in str(e).lower():
                last_error = e
                time.sleep(0.1)
                continue
            else:
                raise
        except json.JSONDecodeError as e:
            # Файл возможно записывается, ждем
            last_error = e
            time.sleep(0.1)
            continue
    
    logger.warning(f"Таймаут чтения файла {file_path}: {last_error}")
    return None

def safe_write_json(file_path, data, timeout=WRITE_TIMEOUT, use_lock=True):
    """
    Безопасная запись JSON файла с блокировкой.
    """
    try:
        # Создаем директорию если её нет
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if use_lock:
            lock_file = f"{file_path}.lock"
            with file_lock(lock_file, timeout):
                return _write_json_atomic(file_path, data)
        else:
            return _write_json_atomic(file_path, data)
            
    except TimeoutError as e:
        logger.error(f"Таймаут записи файла {file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка записи файла {file_path}: {e}")
        return False

def _write_json_atomic(file_path, data):
    """
    Атомарная запись JSON файла.
    """
    temp_name = None
    try:
        file_dir = os.path.dirname(file_path)
        with tempfile.NamedTemporaryFile(mode='w', dir=file_dir, delete=False, encoding='utf-8') as tf:
            json.dump(data, tf, ensure_ascii=False, indent=2)
            temp_name = tf.name
        
        # Атомарное перемещение
        shutil.move(temp_name, file_path)
        return True
        
    except Exception as e:
        logger.error(f"Ошибка атомарной записи {file_path}: {e}")
        if temp_name and os.path.exists(temp_name):
            try:
                os.remove(temp_name)
            except:
                pass
        return False

def initialize_error_handler():
    """Инициализирует обработчик ошибок с callback для состояния"""
    global error_handler
    if error_handler is None:
        error_handler = create_error_handler(update_state_callback=_error_state_callback)
    return error_handler

def _error_state_callback(status, last_error):
    """
    Callback для обновления состояния при ошибках.
    """
    try:
        import datetime
        # вызываем update_global_state напрямую
        update_global_state(
            status=status,
            last_update=datetime.datetime.now().isoformat(),
            last_error=last_error
        )
        logger.debug(f"Состояние обновлено через callback: status={status}")
    except Exception as e:
        logger.warning(f"Не удалось обновить состояние через callback: {e}")

def ensure_local_directories():
    """Создает локальные директории для файлов состояния"""
    directories = [
        os.path.dirname(LOCAL_STATE_FILE),
        os.path.dirname(SERVICE_STATE_FILE),
        os.path.dirname(SERVICE_MINIMAL_FILE),
        os.path.dirname(SUPERVISOR_READ_FILE),
        os.path.dirname(LOCK_FILE)
    ]
    
    for directory in directories:
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Не удалось создать директорию {directory}: {e}")

def load_state():
    """
    ИСПРАВЛЕНО: Загружает состояние с учетом блокировок.
    """
    # Сначала пытаемся загрузить с сетевого хранилища
    if os.path.exists(network_state_file):
        data = safe_read_json(network_state_file)
        if data is not None:
            # Убедимся, что в data есть "global" и "users"
            if "global" not in data:
                data["global"] = {}
            if "users" not in data:
                data["users"] = {}
            logger.debug("Состояние загружено с сетевого хранилища.")
            return data
        else:
            logger.warning("Не удалось прочитать состояние с сетевого хранилища")
    
    # Если сетевой файл недоступен, пытаемся загрузить локальный
    if os.path.exists(LOCAL_STATE_FILE):
        data = safe_read_json(LOCAL_STATE_FILE)
        if data is not None:
            if "global" not in data:
                data["global"] = {}
            if "users" not in data:
                data["users"] = {}
            logger.info("Состояние загружено из локального файла.")
            return data
    
    # Если оба файла недоступны
    logger.warning("Файлы состояния не найдены или недоступны. Используется пустой словарь.")
    return {"global": {}, "users": {}}

def save_to_network(state_dict):
    """
    ИСПРАВЛЕНО: Сохранение на сетевое хранилище с блокировкой.
    """
    try:
        state_dir = os.path.dirname(network_state_file)
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        # Используем блокировку для сетевого файла
        if safe_write_json(network_state_file, state_dict, use_lock=True):
            logger.debug("Состояние сохранено на сетевое хранилище.")
            return True
        else:
            logger.warning("Не удалось сохранить состояние на сетевое хранилище")
            return False
            
    except Exception as e:
        handler = initialize_error_handler()
        handler.handle_error(
            MigrationErrorCodes.NETWORK_001,
            details=f"Критическая ошибка сохранения на сетевое хранилище",
            exception=e,
            context={"file": network_state_file}
        )
        return False

def save_to_local(state_dict):
    """
    ИСПРАВЛЕНО: Локальное сохранение с блокировками и разделением файлов.
    """
    ensure_local_directories()
    
    success_count = 0
    total_files = 4
    
    # 1. Полное состояние в /tmp (для основного скрипта)
    if safe_write_json(LOCAL_STATE_FILE, state_dict, use_lock=True):
        success_count += 1
    
    # 2. Полное состояние для управляющего сервиса
    if safe_write_json(SERVICE_STATE_FILE, state_dict, use_lock=True):
        success_count += 1
    
    # 3. Минимальное состояние для быстрого доступа
    minimal_state = prepare_minimal_state(state_dict)
    if safe_write_json(SERVICE_MINIMAL_FILE, minimal_state, use_lock=True):
        success_count += 1
    
    # 4. НОВОЕ: Отдельный файл для чтения супервизором
    supervisor_state = prepare_supervisor_state(state_dict)
    if safe_write_json(SUPERVISOR_READ_FILE, supervisor_state, use_lock=True):
        success_count += 1
    
    logger.debug(f"Локальное сохранение: {success_count}/{total_files} файлов")
    return success_count > 0  # Считаем успехом если хотя бы один файл сохранился

def prepare_minimal_state(state_dict):
    """
    Готовит минимальное состояние для управляющего сервиса.
    """
    global_state = state_dict.get("global", {})
    users_state = state_dict.get("users", {})
    
    # Подсчитываем статистику пользователей
    total_users = len(users_state)
    users_completed = len([u for u in users_state.values() if u in ["success", "completed_with_error"]])
    users_failed = len([u for u in users_state.values() if u == "failed"])
    users_in_progress = len([u for u in users_state.values() if u == "in_progress"])
    
    # Вычисляем общий прогресс
    overall_progress = (users_completed / total_users * 100) if total_users > 0 else 0
    
    # Находим текущего пользователя
    current_user = None
    for user, status in users_state.items():
        if status == "in_progress":
            current_user = user
            break
    
    return {
        "service_timestamp": datetime.datetime.now().isoformat(),
        "status": global_state.get("status", "unknown"),
        "last_update": global_state.get("last_update"),
        "last_heartbeat": global_state.get("last_heartbeat"),
        "overall_progress": overall_progress,
        "current_user": current_user,
        "total_users": total_users,
        "users_completed": users_completed,
        "users_failed": users_failed,
        "users_in_progress": users_in_progress,
        "last_error": global_state.get("last_error"),
        "script_version": config.get("SCRIPT_VERSION"),
        "data_source_type": config.get("DATA_SOURCE_TYPE")
    }

def prepare_supervisor_state(state_dict):
    """
    НОВОЕ: Готовит состояние специально для супервизора.
    Включает только информацию, необходимую для мониторинга.
    """
    global_state = state_dict.get("global", {})
    users_state = state_dict.get("users", {})
    
    # Базовая статистика
    total_users = len(users_state)
    users_completed = len([u for u in users_state.values() if u in ["success", "completed_with_error"]])
    users_in_progress = len([u for u in users_state.values() if u == "in_progress"])
    
    return {
        "supervisor_timestamp": datetime.datetime.now().isoformat(),
        "status": global_state.get("status", "unknown"),
        "last_heartbeat": global_state.get("last_heartbeat"),
        "current_user": global_state.get("current_user"),
        "users_in_progress": users_in_progress,
        "progress_percent": (users_completed / total_users * 100) if total_users > 0 else 0,
        "last_error": global_state.get("last_error", {}).get("code") if global_state.get("last_error") else None
    }

def save_state_dual(state_dict):
    """
    ИСПРАВЛЕНО: Сохранение с приоритетом локальных файлов.
    """
    # Всегда сохраняем локально (критически важно)
    local_success = save_to_local(state_dict)
    
    # Пытаемся сохранить на сетевое хранилище (для внешнего мониторинга)
    network_success = save_to_network(state_dict)
    
    if local_success and network_success:
        logger.debug("Состояние сохранено локально и на сетевом хранилище.")
    elif local_success:
        logger.debug("Состояние сохранено локально. Сетевое хранилище недоступно.")
    elif network_success:
        logger.warning("Состояние сохранено только на сетевом хранилище.")
    else:
        logger.error("Не удалось сохранить состояние!")
    
    return local_success or network_success

def update_global_state(**kwargs):
    """
    ИСПРАВЛЕНО: Обновление с защитой от блокировок.
    """
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            state = load_state()
            global_state = state.get("global", {})
            
            # Добавляем timestamp обновления
            kwargs["last_update"] = datetime.datetime.now().isoformat()
            
            for k, v in kwargs.items():
                global_state[k] = v
            state["global"] = global_state

            # Используем двойное сохранение
            if save_state_dual(state):
                return  # Успех
            
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} обновления состояния не удалась: {e}")
            
        if attempt < max_retries - 1:
            time.sleep(retry_delay * (attempt + 1))
    
    logger.error("Не удалось обновить глобальное состояние после всех попыток")

def update_user_state(user, status):
    """
    ИСПРАВЛЕНО: Обновление состояния пользователя с защитой от блокировок.
    """
    max_retries = 3
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            state = load_state()
            users = state.get("users", {})
            users[user] = status
            state["users"] = users

            # Обновляем глобальную информацию
            global_state = state.get("global", {})
            global_state["last_update"] = datetime.datetime.now().isoformat()
            
            # Добавляем информацию о текущем пользователе
            if status == "in_progress":
                global_state["current_user"] = user
            elif global_state.get("current_user") == user and status in ["success", "failed", "completed_with_error"]:
                global_state["current_user"] = None
            
            state["global"] = global_state

            # Используем двойное сохранение
            if save_state_dual(state):
                return  # Успех
                
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} обновления состояния пользователя не удалась: {e}")
            
        if attempt < max_retries - 1:
            time.sleep(retry_delay * (attempt + 1))
    
    logger.error(f"Не удалось обновить состояние пользователя {user} после всех попыток")

def get_local_state_for_service():
    """
    ИСПРАВЛЕНО: Чтение состояния для управляющего сервиса с защитой от блокировок.
    """
    # Приоритет файлов для чтения
    files_to_try = [
        (SERVICE_MINIMAL_FILE, "минимальный файл", lambda x: x),
        (SUPERVISOR_READ_FILE, "файл супервизора", lambda x: x),
        (SERVICE_STATE_FILE, "полный файл сервиса", prepare_minimal_state),
        (LOCAL_STATE_FILE, "файл /tmp", prepare_minimal_state)
    ]
    
    for file_path, description, processor in files_to_try:
        data = safe_read_json(file_path, timeout=2.0)  # Короткий таймаут для сервиса
        if data is not None:
            try:
                result = processor(data)
                logger.debug(f"Состояние прочитано из {description}")
                return result
            except Exception as e:
                logger.warning(f"Ошибка обработки {description}: {e}")
                continue
    
    # Если все файлы недоступны, возвращаем заглушку
    logger.warning("Все локальные файлы состояния недоступны")
    return {
        "service_timestamp": datetime.datetime.now().isoformat(),
        "status": "unknown",
        "last_update": None,
        "last_heartbeat": None,
        "overall_progress": 0,
        "current_user": None,
        "total_users": 0,
        "users_completed": 0,
        "users_failed": 0,
        "users_in_progress": 0,
        "last_error": None
    }

# Сохраняем существующие функции для обратной совместимости
def save_full_state(state_dict):
    """Для обратной совместимости"""
    return save_to_network(state_dict)

def cleanup_old_state_files():
    """
    ИСПРАВЛЕНО: Очистка с проверкой блокировок.
    """
    files_to_clean = [
        LOCAL_STATE_FILE, 
        SERVICE_STATE_FILE, 
        SERVICE_MINIMAL_FILE,
        SUPERVISOR_READ_FILE
    ]
    
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            try:
                # Проверяем, не заблокирован ли файл
                with file_lock(f"{file_path}.lock", timeout=1.0):
                    os.remove(file_path)
                    logger.info(f"Удален старый файл состояния: {file_path}")
            except (TimeoutError, Exception) as e:
                logger.warning(f"Не удалось удалить файл {file_path}: {e}")

# Дополнительные функции для интеграции с управляющим сервисом
def is_migration_active():
    """Проверяет, активна ли миграция"""
    try:
        state = get_local_state_for_service()
        return state.get("status") in ["in_progress", "starting"]
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка проверки активности миграции",
            exception=e
        )
        return False

def get_migration_progress():
    """Возвращает прогресс миграции в процентах"""
    try:
        state = get_local_state_for_service()
        return state.get("overall_progress", 0)
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка получения прогресса миграции",
            exception=e
        )
        return 0

def get_last_heartbeat():
    """Возвращает время последнего heartbeat"""
    try:
        state = get_local_state_for_service()
        heartbeat_str = state.get("last_heartbeat")
        if heartbeat_str:
            return datetime.datetime.fromisoformat(heartbeat_str)
        return None
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка получения последнего heartbeat",
            exception=e
        )
        return None

def force_network_sync():
    """Принудительно синхронизирует состояние с сетевым хранилищем"""
    try:
        data = safe_read_json(LOCAL_STATE_FILE, timeout=5.0)
        if data and save_to_network(data):
            logger.info("Принудительная синхронизация выполнена успешно.")
            return True
        
        handle_migration_error(
            MigrationErrorCodes.NETWORK_001,
            details="Не удалось выполнить принудительную синхронизацию"
        )
        return False
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details="Критическая ошибка принудительной синхронизации",
            exception=e
        )
        return False

def get_error_summary():
    """Возвращает сводку по ошибкам миграции"""
    handler = initialize_error_handler()
    return handler.get_error_summary()

def handle_migration_error(error_code, details="", exception=None, context=None):
    """Обработка ошибок миграции с обновлением состояния"""
    from src.errors.error_codes import ErrorHandler
    handler = ErrorHandler(update_state_callback=None)
    
    error_info = handler.handle_error(error_code, details, exception, context)
    
    # Обновляем глобальное состояние (с защитой от блокировок)
    try:
        current_state = load_state()
        current_status = current_state.get("global", {}).get("status", "unknown")
        
        should_fail = False
        critical_categories = ["INIT", "CONFIG", "MOUNT", "SOURCE"]
        
        if error_code.severity == "CRITICAL":
            should_fail = True
        elif error_code.category.value in critical_categories:
            should_fail = True
        elif current_status == "failed":
            should_fail = True
        
        if should_fail:
            update_global_state(
                status="failed",
                last_update=datetime.datetime.now().isoformat(),
                last_error=error_info
            )
            logger.error(f"Миграция остановлена из-за критической ошибки {error_code.code}")
        else:
            update_global_state(
                last_update=datetime.datetime.now().isoformat(),
                last_error=error_info
            )
            logger.warning(f"Зафиксирована ошибка {error_code.code}, миграция продолжается")
            
    except Exception as e:
        logger.warning(f"Не удалось обновить состояние при ошибке {error_code.code}: {e}")
    
    return error_info