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

import json
import os
import logging
import tempfile
import shutil
import datetime
from pathlib import Path
from src.logging.logger import setup_logger
from src.config.config_loader import load_config
from src.errors.error_codes import ErrorHandler, MigrationErrorCodes, create_error_handler

error_handler = None

setup_logger()
logger = logging.getLogger(__name__)

# Загружаем конфиг
config = load_config()
network_state_file = config["STATE_FILE"]  # /mnt/.../migration_state.json (на сетевом хранилище)

# Локальные файлы состояния для управляющего сервиса
LOCAL_STATE_FILE = "/tmp/migration_state.json"  # Полное состояние для основного скрипта
SERVICE_STATE_FILE = "/var/lib/migration-service/state.json"  # Основной файл состояния сервиса
SERVICE_MINIMAL_FILE = "/var/lib/migration-service/current_state.json"  # Минимальное состояние для быстрого доступа

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
    try:
        Path(LOCAL_STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(SERVICE_STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(SERVICE_MINIMAL_FILE).parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.warning(f"Не удалось создать локальные директории: {e}")


def load_state():
    """
    Загружает текущее состояние миграции из network_state_file.
    Если сетевой файл недоступен, пытается загрузить из локального файла.
    Если ни один не найден, возвращаем структуру с {"global":{}, "users":{}}.
    """
    # Сначала пытаемся загрузить с сетевого хранилища
    if os.path.exists(network_state_file):
        try:
            with open(network_state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Убедимся, что в data есть "global" и "users"
            if "global" not in data:
                data["global"] = {}
            if "users" not in data:
                data["users"] = {}
            logger.debug("Состояние миграции загружено с сетевого хранилища.")
            return data
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            handler = initialize_error_handler()
            handler.handle_error(
                MigrationErrorCodes.SOURCE_003,
                details=f"Файл состояния поврежден на сетевом хранилище",
                exception=e,
                context={"file": network_state_file}
            )
        except Exception as e:
            handler = initialize_error_handler()
            handler.handle_error(
                MigrationErrorCodes.NETWORK_001,
                details=f"Ошибка загрузки состояния с сетевого хранилища",
                exception=e,
                context={"file": network_state_file}
            )
    
    # Если сетевой файл недоступен, пытаемся загрузить локальный
    if os.path.exists(LOCAL_STATE_FILE):
        try:
            with open(LOCAL_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "global" not in data:
                data["global"] = {}
            if "users" not in data:
                data["users"] = {}
            logger.info("Состояние миграции загружено из локального файла.")
            return data
        except Exception as e:
            handler = initialize_error_handler()
            handler.handle_error(
                MigrationErrorCodes.SOURCE_003,
                details=f"Ошибка загрузки локального состояния",
                exception=e,
                context={"file": LOCAL_STATE_FILE}
            )
    
    # Если оба файла недоступны
    logger.warning("Файлы состояния не найдены. Будет использован пустой словарь.")
    return {"global": {}, "users": {}}


def save_to_network(state_dict):
    """
    Сохраняет состояние на сетевое хранилище.
    Возвращает True при успехе, False при ошибке.
    """
    try:
        state_dir = os.path.dirname(network_state_file)
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        temp_name = None
        with tempfile.NamedTemporaryFile(mode='w', dir=state_dir, delete=False) as tf:
            json.dump(state_dict, tf, ensure_ascii=False, indent=2)
            temp_name = tf.name
        shutil.move(temp_name, network_state_file)
        logger.debug("Состояние миграции сохранено на сетевое хранилище.")
        return True
    except PermissionError as e:
        # Используем коды ошибок для стандартизированной обработки
        handler = initialize_error_handler()
        handler.handle_error(
            MigrationErrorCodes.TARGET_003,
            details=f"Нет прав для записи в {network_state_file}",
            exception=e,
            context={"file": network_state_file, "operation": "network_save"}
        )
        return False
    except OSError as e:
        handler = initialize_error_handler()
        if "No space left" in str(e):
            handler.handle_error(
                MigrationErrorCodes.TARGET_002,
                details=f"Недостаточно места для сохранения состояния",
                exception=e,
                context={"file": network_state_file}
            )
        else:
            handler.handle_error(
                MigrationErrorCodes.NETWORK_001,
                details=f"Сетевая ошибка при сохранении состояния",
                exception=e,
                context={"file": network_state_file}
            )
        return False
    except Exception as e:
        handler = initialize_error_handler()
        handler.handle_error(
            MigrationErrorCodes.SYSTEM_003,
            details=f"Неожиданная ошибка при сохранении на сетевое хранилище",
            exception=e,
            context={"file": network_state_file}
        )
        if temp_name and os.path.exists(temp_name):
            try:
                os.remove(temp_name)
            except:
                pass
        return False

def save_to_local(state_dict):
    """
    Сохраняет состояние локально в несколько файлов.
    Возвращает True при успехе, False при ошибке.
    """
    ensure_local_directories()
    
    temp_files = []
    try:
        # 1. Полное состояние в /tmp (для основного скрипта и совместимости)
        temp_name = None
        temp_dir = os.path.dirname(LOCAL_STATE_FILE)
        with tempfile.NamedTemporaryFile(mode='w', dir=temp_dir, delete=False) as tf:
            json.dump(state_dict, tf, ensure_ascii=False, indent=2)
            temp_name = tf.name
        temp_files.append(temp_name)
        shutil.move(temp_name, LOCAL_STATE_FILE)
        
        # 2. Полное состояние для управляющего сервиса
        temp_name_service = None
        service_dir = os.path.dirname(SERVICE_STATE_FILE)
        with tempfile.NamedTemporaryFile(mode='w', dir=service_dir, delete=False) as tf:
            json.dump(state_dict, tf, ensure_ascii=False, indent=2)
            temp_name_service = tf.name
        temp_files.append(temp_name_service)
        shutil.move(temp_name_service, SERVICE_STATE_FILE)
        
        # 3. Минимальное состояние для быстрого доступа управляющего сервиса
        minimal_state = prepare_minimal_state(state_dict)
        temp_name_minimal = None
        with tempfile.NamedTemporaryFile(mode='w', dir=service_dir, delete=False) as tf:
            json.dump(minimal_state, tf, ensure_ascii=False, indent=2)
            temp_name_minimal = tf.name
        temp_files.append(temp_name_minimal)
        shutil.move(temp_name_minimal, SERVICE_MINIMAL_FILE)
        
        logger.debug("Состояние миграции сохранено локально во все файлы.")
        return True
        
    except PermissionError as e:
        handler = initialize_error_handler()
        handler.handle_error(
            MigrationErrorCodes.TARGET_003,
            details=f"Нет прав для записи локальных файлов состояния",
            exception=e,
            context={"files": [LOCAL_STATE_FILE, SERVICE_STATE_FILE, SERVICE_MINIMAL_FILE]}
        )
        return False
    except OSError as e:
        handler = initialize_error_handler()
        if "No space left" in str(e):
            handler.handle_error(
                MigrationErrorCodes.TARGET_002,
                details=f"Недостаточно места для локальных файлов состояния",
                exception=e
            )
        else:
            handler.handle_error(
                MigrationErrorCodes.SYSTEM_002,
                details=f"Системная ошибка при сохранении локальных файлов",
                exception=e
            )
        return False
    except Exception as e:
        handler = initialize_error_handler()
        handler.handle_error(
            MigrationErrorCodes.SYSTEM_003,
            details=f"Критическая ошибка локального сохранения",
            exception=e
        )
        return False
    finally:
        # Очистка временных файлов при ошибке
        for temp_file in temp_files:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass

def prepare_minimal_state(state_dict):
    """
    Готовит минимальное состояние для управляющего сервиса.
    Включает только самую важную информацию.
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
    
    # Находим текущего пользователя (если есть)
    current_user = None
    for user, status in users_state.items():
        if status == "in_progress":
            current_user = user
            break
    
    minimal_state = {
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
    
    return minimal_state

def save_full_state(state_dict):
    """
    Перезаписывает файл состояния целиком на сетевом хранилище.
    Для обратной совместимости - использует только сетевое хранилище.
    """
    save_to_network(state_dict)

def save_state_dual(state_dict):
    """
    Сохраняет состояние одновременно локально и на сетевом хранилище.
    Локальное сохранение имеет приоритет для стабильности работы управляющего сервиса.
    """
    # Всегда сохраняем локально (критически важно для управляющего сервиса)
    local_success = save_to_local(state_dict)
    
    # Пытаемся сохранить на сетевое хранилище (для внешнего мониторинга)
    network_success = save_to_network(state_dict)
    
    if local_success and network_success:
        logger.debug("Состояние сохранено локально и на сетевом хранилище.")
    elif local_success:
        logger.debug("Состояние сохранено локально. Сетевое хранилище недоступно.")
    elif network_success:
        logger.warning("Состояние сохранено только на сетевом хранилище. Локальное сохранение не удалось.")
    else:
        logger.error("Не удалось сохранить состояние ни локально, ни на сетевом хранилище!")
    
    return local_success or network_success

def update_global_state(**kwargs):
    """
    Обновляет поля в state["global"].
    Использует двойное сохранение для надежности.
    """
    state = load_state()
    global_state = state.get("global", {})
    
    # Добавляем timestamp обновления
    kwargs["last_update"] = datetime.datetime.now().isoformat()
    
    for k, v in kwargs.items():
        global_state[k] = v
    state["global"] = global_state

    # Используем двойное сохранение
    save_state_dual(state)

def update_user_state(user, status):
    """
    Устанавливает state["users"][user] = status.
    Использует двойное сохранение для надежности.
    """
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
        # Пользователь завершился, очищаем current_user
        global_state["current_user"] = None
    
    state["global"] = global_state

    # Используем двойное сохранение
    save_state_dual(state)

def get_local_state_for_service():
    """
    Возвращает локальное состояние для управляющего сервиса.
    Читает файлы в порядке приоритета: минимальный -> полный сервисный -> /tmp -> заглушка.
    """
    # 1. Пытаемся прочитать минимальный файл состояния (самый быстрый)
    if os.path.exists(SERVICE_MINIMAL_FILE):
        try:
            with open(SERVICE_MINIMAL_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                logger.debug("Состояние прочитано из минимального файла")
                return state
        except Exception as e:
            logger.warning(f"Ошибка чтения минимального состояния: {e}")
    
    # 2. Fallback - читаем полное состояние сервиса
    if os.path.exists(SERVICE_STATE_FILE):
        try:
            with open(SERVICE_STATE_FILE, "r", encoding="utf-8") as f:
                full_state = json.load(f)
                logger.debug("Состояние прочитано из полного файла сервиса")
                return prepare_minimal_state(full_state)
        except Exception as e:
            logger.warning(f"Ошибка чтения полного состояния сервиса: {e}")
    
    # 3. Fallback - читаем состояние из /tmp
    if os.path.exists(LOCAL_STATE_FILE):
        try:
            with open(LOCAL_STATE_FILE, "r", encoding="utf-8") as f:
                full_state = json.load(f)
                logger.debug("Состояние прочитано из /tmp файла")
                return prepare_minimal_state(full_state)
        except Exception as e:
            logger.warning(f"Ошибка чтения состояния из /tmp: {e}")
    
    # 4. Если все файлы недоступны, возвращаем заглушку
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

def cleanup_old_state_files():
    """
    Утилита для очистки старых файлов состояния.
    Можно вызывать при запуске новой миграции.
    """
    files_to_clean = [LOCAL_STATE_FILE, SERVICE_STATE_FILE, SERVICE_MINIMAL_FILE]
    
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Удален старый файл состояния: {file_path}")
            except Exception as e:
                logger.warning(f"Не удалось удалить файл {file_path}: {e}")

# Дополнительные функции для интеграции с управляющим сервисом
def is_migration_active():
    """
    Проверяет, активна ли миграция в данный момент.
    """
    try:
        state = get_local_state_for_service()
        return state.get("status") in ["in_progress", "starting"]
    except Exception as e:
        # ДОБАВЛЯЕМ обработку ошибок
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка проверки активности миграции",
            exception=e
        )
        return False

def get_migration_progress():
    """
    Возвращает прогресс миграции в процентах.
    """
    try:
        state = get_local_state_for_service()
        return state.get("overall_progress", 0)
    except Exception as e:
        # ДОБАВЛЯЕМ обработку ошибок
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка получения прогресса миграции",
            exception=e
        )
        return 0

def get_last_heartbeat():
    """
    Возвращает время последнего heartbeat.
    """
    try:
        state = get_local_state_for_service()
        heartbeat_str = state.get("last_heartbeat")
        if heartbeat_str:
            return datetime.datetime.fromisoformat(heartbeat_str)
        return None
    except Exception as e:
        # ДОБАВЛЯЕМ обработку ошибок
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка получения последнего heartbeat",
            exception=e
        )
        return None

def force_network_sync():
    """
    Принудительно синхронизирует локальное состояние с сетевым хранилищем.
    Полезно при восстановлении после сбоев.
    """
    try:
        if os.path.exists(LOCAL_STATE_FILE):
            with open(LOCAL_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            if save_to_network(state):
                logger.info("Принудительная синхронизация с сетевым хранилищем выполнена успешно.")
                return True
        handle_migration_error(
            MigrationErrorCodes.NETWORK_001,
            details="Не удалось выполнить принудительную синхронизацию"
        )
        return False
    except Exception as e:
        # ДОБАВЛЯЕМ обработку ошибок
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
    """   
    Args:
        error_code: Код ошибки из MigrationErrorCodes
        details: Дополнительные детали ошибки
        exception: Исключение Python (если есть)
        context: Контекст ошибки (пользователь, файл и т.д.)
        
    Returns:
        Структурированная информация об ошибке
    """
    # Создаем handler БЕЗ callback чтобы избежать двойных вызовов
    from src.errors.error_codes import ErrorHandler
    handler = ErrorHandler(update_state_callback=None)
    
    # Обрабатываем ошибку
    error_info = handler.handle_error(error_code, details, exception, context)
    
    # ИНТЕГРАЦИЯ: автоматически обновляем глобальное состояние
    try:
        import datetime
        
        # Логика определения нового статуса на основе типа ошибки
        current_state = load_state()
        current_status = current_state.get("global", {}).get("status", "unknown")
        
        # Определяем, нужно ли менять статус
        should_fail = False
        
        # Критические ошибки, которые должны остановить миграцию
        critical_categories = [
            "INIT", "CONFIG", "MOUNT", "SOURCE"  # Ошибки, при которых миграция не может продолжаться
        ]
        
        # Ошибки уровня CRITICAL всегда останавливают миграцию
        if error_code.severity == "CRITICAL":
            should_fail = True
        # Ошибки критических категорий останавливают миграцию
        elif error_code.category.value in critical_categories:
            should_fail = True
        # Если уже в статусе failed, не меняем
        elif current_status == "failed":
            should_fail = True
        
        # Обновляем состояние
        if should_fail:
            update_global_state(
                status="failed",
                last_update=datetime.datetime.now().isoformat(),
                last_error=error_info
            )
            logger.error(f"Миграция остановлена из-за критической ошибки {error_code.code}")
        else:
            # Для некритических ошибок просто записываем информацию об ошибке
            update_global_state(
                last_update=datetime.datetime.now().isoformat(),
                last_error=error_info
            )
            logger.warning(f"Зафиксирована ошибка {error_code.code}, миграция продолжается")
            
    except Exception as e:
        logger.warning(f"Не удалось обновить состояние при ошибке {error_code.code}: {e}")
    
    return error_info