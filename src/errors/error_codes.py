"""
Модуль кодов ошибок для миграции данных.

Предоставляет стандартизированные коды ошибок для аналитики и отладки.
Каждый код включает категорию, описание и рекомендации по устранению.
"""

import logging
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Категории ошибок для группировки"""
    INIT = "INIT"           # Ошибки инициализации
    MOUNT = "MOUNT"         # Ошибки монтирования
    SOURCE = "SOURCE"       # Ошибки источника данных
    TARGET = "TARGET"       # Ошибки целевой системы
    COPY = "COPY"           # Ошибки копирования файлов
    VERIFY = "VERIFY"       # Ошибки проверки целостности
    USER = "USER"           # Ошибки пользователей
    NETWORK = "NETWORK"     # Сетевые ошибки
    SYSTEM = "SYSTEM"       # Системные ошибки
    CONFIG = "CONFIG"       # Ошибки конфигурации


@dataclass
class ErrorCode:
    """Структура кода ошибки"""
    code: str
    category: ErrorCategory
    description: str
    solution: str
    severity: str = "ERROR"  # ERROR, WARNING, CRITICAL


class MigrationErrorCodes:
    """Централизованный реестр кодов ошибок миграции"""
    
    # Инициализация и конфигурация
    INIT_001 = ErrorCode(
        "INIT_001", ErrorCategory.INIT,
        "Ошибка загрузки конфигурационного файла",
        "Проверьте синтаксис YAML файла и права доступа"
    )
    
    INIT_002 = ErrorCode(
        "INIT_002", ErrorCategory.INIT,
        "Отсутствует обязательный параметр конфигурации",
        "Добавьте недостающий параметр в settings.yaml"
    )
    
    CONFIG_001 = ErrorCode(
        "CONFIG_001", ErrorCategory.CONFIG,
        "Неподдерживаемый тип источника данных",
        "Используйте 'network' или 'usb' в параметре DATA_SOURCE_TYPE"
    )
    
    # Монтирование и подключения
    MOUNT_001 = ErrorCode(
        "MOUNT_001", ErrorCategory.MOUNT,
        "Не удалось смонтировать сетевое хранилище",
        "Проверьте сетевое подключение и credentials"
    )
    
    MOUNT_002 = ErrorCode(
        "MOUNT_002", ErrorCategory.MOUNT,
        "Не удалось смонтировать USB накопитель",
        "Проверьте подключение USB устройства и его файловую систему"
    )
    
    MOUNT_003 = ErrorCode(
        "MOUNT_003", ErrorCategory.MOUNT,
        "Превышено максимальное количество попыток монтирования",
        "Проверьте доступность ресурса и увеличьте MOUNT_ATTEMPTS"
    )
    
    # Ошибки источника данных
    SOURCE_001 = ErrorCode(
        "SOURCE_001", ErrorCategory.SOURCE,
        "Исходная папка не найдена или недоступна",
        "Проверьте путь к источнику и права доступа"
    )
    
    SOURCE_002 = ErrorCode(
        "SOURCE_002", ErrorCategory.SOURCE,
        "Директория пользователя не найдена",
        "Убедитесь, что данные пользователя присутствуют в источнике"
    )
    
    SOURCE_003 = ErrorCode(
        "SOURCE_003", ErrorCategory.SOURCE,
        "Не удалось прочитать файл из источника",
        "Проверьте целостность файловой системы источника"
    )
    
    # Ошибки целевой системы
    TARGET_001 = ErrorCode(
        "TARGET_001", ErrorCategory.TARGET,
        "Не удалось создать целевую директорию",
        "Проверьте права доступа и свободное место на диске"
    )
    
    TARGET_002 = ErrorCode(
        "TARGET_002", ErrorCategory.TARGET,
        "Недостаточно места на целевом диске",
        "Освободите место или выберите другой целевой диск"
    )
    
    TARGET_003 = ErrorCode(
        "TARGET_003", ErrorCategory.TARGET,
        "Не удалось установить права доступа",
        "Проверьте права администратора и существование пользователя"
    )
    
    # Ошибки копирования
    COPY_001 = ErrorCode(
        "COPY_001", ErrorCategory.COPY,
        "Ошибка копирования файла",
        "Проверьте свободное место и права доступа к файлу"
    )
    
    COPY_002 = ErrorCode(
        "COPY_002", ErrorCategory.COPY,
        "Файл заблокирован другим процессом",
        "Закройте приложения, использующие файл, и повторите"
    )
    
    COPY_003 = ErrorCode(
        "COPY_003", ErrorCategory.COPY,
        "Превышен максимальный размер файла",
        "Проверьте ограничения файловой системы"
    )
    
    # Ошибки проверки целостности
    VERIFY_001 = ErrorCode(
        "VERIFY_001", ErrorCategory.VERIFY,
        "Несовпадение хеш-суммы файла",
        "Файл поврежден, повторите копирование"
    )
    
    VERIFY_002 = ErrorCode(
        "VERIFY_002", ErrorCategory.VERIFY,
        "Не удалось вычислить хеш-сумму",
        "Проверьте доступность файла и права на чтение"
    )
    
    VERIFY_003 = ErrorCode(
        "VERIFY_003", ErrorCategory.VERIFY,
        "Несовпадение размера файла",
        "Файл скопирован не полностью, повторите операцию"
    )
    
    # Ошибки пользователей
    USER_001 = ErrorCode(
        "USER_001", ErrorCategory.USER,
        "Не удалось создать пользователя Linux",
        "Проверьте права администратора и уникальность имени"
    )
    
    USER_002 = ErrorCode(
        "USER_002", ErrorCategory.USER,
        "Ошибка форматирования имени пользователя",
        "Имя содержит недопустимые символы для Linux"
    )
    
    USER_003 = ErrorCode(
        "USER_003", ErrorCategory.USER,
        "Ошибка миграции пользователя",
        "Проверьте логи для конкретной причины сбоя"
    )
    
    # Сетевые ошибки
    NETWORK_001 = ErrorCode(
        "NETWORK_001", ErrorCategory.NETWORK,
        "Потеря сетевого соединения",
        "Проверьте стабильность сетевого подключения"
    )
    
    NETWORK_002 = ErrorCode(
        "NETWORK_002", ErrorCategory.NETWORK,
        "Тайм-аут сетевой операции",
        "Увеличьте время ожидания или проверьте скорость сети"
    )
    
    NETWORK_003 = ErrorCode(
        "NETWORK_003", ErrorCategory.NETWORK,
        "Ошибка аутентификации на сетевом ресурсе",
        "Проверьте учетные данные в конфигурации"
    )
    
    # Системные ошибки
    SYSTEM_001 = ErrorCode(
        "SYSTEM_001", ErrorCategory.SYSTEM,
        "Недостаточно памяти",
        "Закройте лишние приложения или добавьте RAM"
    )
    
    SYSTEM_002 = ErrorCode(
        "SYSTEM_002", ErrorCategory.SYSTEM,
        "Ошибка доступа к системному ресурсу",
        "Запустите скрипт с правами администратора"
    )
    
    SYSTEM_003 = ErrorCode(
        "SYSTEM_003", ErrorCategory.SYSTEM,
        "Критическая системная ошибка",
        "Перезагрузите систему и повторите миграцию"
    )


class ErrorHandler:
    """Обработчик ошибок с логированием и состоянием"""
    
    def __init__(self, update_state_callback=None):
        self.update_state_callback = update_state_callback
        self.error_stats = {category.value: 0 for category in ErrorCategory}
        
    def handle_error(self, error_code: ErrorCode, details: str = "", 
                    exception: Optional[Exception] = None, 
                    context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Обрабатывает ошибку с логированием и обновлением состояния
        
        Args:
            error_code: Код ошибки из MigrationErrorCodes
            details: Дополнительные детали ошибки
            exception: Исключение Python (если есть)
            context: Контекст ошибки (пользователь, файл и т.д.)
            
        Returns:
            Структурированная информация об ошибке
        """
        # Формируем структурированную ошибку
        error_info = {
            "code": error_code.code,
            "category": error_code.category.value,
            "description": error_code.description,
            "solution": error_code.solution,
            "details": details,
            "severity": error_code.severity,
            "context": context or {},
            "timestamp": self._get_timestamp()
        }
        
        # Добавляем информацию об исключении
        if exception:
            error_info["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": self._get_traceback_string(exception)
            }
        
        # Логируем ошибку
        self._log_error(error_info)
        
        # Обновляем статистику
        self.error_stats[error_code.category.value] += 1
        
        # Обновляем состояние миграции (если задан callback)
        if self.update_state_callback:
            self.update_state_callback(
                status="failed" if error_code.severity == "CRITICAL" else "error",
                last_error=error_info
            )
        
        return error_info
    
    def _log_error(self, error_info: Dict[str, Any]):
        """Логирует ошибку с соответствующим уровнем"""
        severity = error_info["severity"]
        message = f"[{error_info['code']}] {error_info['description']}"
        
        if error_info["details"]:
            message += f" | Детали: {error_info['details']}"
            
        if error_info["context"]:
            message += f" | Контекст: {error_info['context']}"
        
        if severity == "CRITICAL":
            logger.critical(message)
        elif severity == "ERROR":
            logger.error(message)
        else:
            logger.warning(message)
    
    def _get_timestamp(self) -> str:
        """Возвращает текущую метку времени"""
        import datetime
        return datetime.datetime.now().isoformat()
    
    def _get_traceback_string(self, exception: Exception) -> str:
        """Получает строковое представление traceback"""
        import traceback
        return ''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Возвращает сводку по ошибкам"""
        total_errors = sum(self.error_stats.values())
        return {
            "total_errors": total_errors,
            "by_category": self.error_stats.copy(),
            "most_frequent": max(self.error_stats.items(), key=lambda x: x[1]) if total_errors > 0 else None
        }


# Удобные функции для быстрого использования
def create_error_handler(update_state_callback=None) -> ErrorHandler:
    """Создает обработчик ошибок"""
    return ErrorHandler(update_state_callback)


def get_error_by_code(code: str) -> Optional[ErrorCode]:
    """Находит код ошибки по строковому идентификатору"""
    for attr_name in dir(MigrationErrorCodes):
        if not attr_name.startswith('_'):
            error_code = getattr(MigrationErrorCodes, attr_name)
            if isinstance(error_code, ErrorCode) and error_code.code == code:
                return error_code
    return None


def format_error_for_user(error_info: Dict[str, Any]) -> str:
    """Форматирует ошибку для отображения пользователю"""
    return f"Ошибка {error_info['code']}: {error_info['description']}. " \
           f"Рекомендация: {error_info.get('solution', 'Обратитесь к администратору')}"


# Декоратор для автоматической обработки ошибок
def handle_migration_errors(error_handler: ErrorHandler, default_error: ErrorCode):
    """Декоратор для автоматической обработки ошибок в функциях"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_handler.handle_error(
                    default_error,
                    details=f"Ошибка в функции {func.__name__}",
                    exception=e,
                    context={"function": func.__name__, "args": str(args)[:100]}
                )
                raise
        return wrapper
    return decorator