"""
Модуль для нормализации структуры директорий.

Функции:
    - get_users_from_host_dir: Получение списка пользователей из директории хоста.
    - format_username_for_linux: Форматирование имени пользователя для Linux.
    - set_permissions: Установка прав доступа для директорий и файлов.
    - copy_skel: Копирование скелета пользователя.
"""
import os
import logging
import grp
import pwd
import shutil
from typing import List
from src.logging.logger import setup_logger
from src.config.config_loader import load_config
from src.errors.error_codes import MigrationErrorCodes
from src.migration.state_tracker import handle_migration_error


logger = logging.getLogger(__name__)
config = load_config()
domains = config.get("DOMAINS", {})

def get_users_from_host_dir(host_dir: str, exclude_dirs: List[str] = None) -> List[str]:
    """
    Получение списка пользователей из директории хоста.

    :param host_dir: Директория хоста.
    :return: Список пользователей.
    """
    exclude_dirs = exclude_dirs or []
    try:
        if not os.path.exists(host_dir):
            handle_migration_error(
                MigrationErrorCodes.SOURCE_001,
                details=f"Директория хоста не найдена: {host_dir}",
                context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
            )
            return []
        
        if not os.path.isdir(host_dir):
            handle_migration_error(
                MigrationErrorCodes.SOURCE_001,
                details=f"Путь не является директорией: {host_dir}",
                context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
            )
            return []
        
        users = [
            d for d in os.listdir(host_dir)
            if os.path.isdir(os.path.join(host_dir, d)) and d not in exclude_dirs
        ]
        
        logger.info(f"Найдено пользователей: {len(users)}")
        return users
        
    except FileNotFoundError as e:
        handle_migration_error(
            MigrationErrorCodes.SOURCE_001,
            details=f"Директория {host_dir} не найдена",
            exception=e,
            context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
        )
        return []
        
    except PermissionError as e:
        handle_migration_error(
            MigrationErrorCodes.SOURCE_003,
            details=f"Недостаточно прав для доступа к директории {host_dir}",
            exception=e,
            context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
        )
        return []
        
    except NotADirectoryError as e:
        handle_migration_error(
            MigrationErrorCodes.SOURCE_001,
            details=f"Путь {host_dir} не является директорией",
            exception=e,
            context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
        )
        return []
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SOURCE_003,
            details=f"Неожиданная ошибка при получении списка пользователей из {host_dir}",
            exception=e,
            context={"host_dir": host_dir, "function": "get_users_from_host_dir"}
        )
        return []

def format_username_for_linux(username: str) -> str:
    """
    Приведение имени пользователя к виду для Linux.

    :param username: Имя пользователя с доменным суффиксом.
    :return: Приведенное имя пользователя.
    """
    try:
        if not username:
            handle_migration_error(
                MigrationErrorCodes.USER_002,
                details="Имя пользователя не может быть пустым",
                context={"username": username, "function": "format_username_for_linux"}
            )
            return ''
        
        if not isinstance(username, str):
            handle_migration_error(
                MigrationErrorCodes.USER_002,
                details=f"Имя пользователя должно быть строкой, получено: {type(username)}",
                context={"username": username, "function": "format_username_for_linux"}
            )
            return ''
        
        # Проверяем на недопустимые символы для Linux
        invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', ' ']
        if any(char in username for char in invalid_chars):
            handle_migration_error(
                MigrationErrorCodes.USER_002,
                details=f"Имя пользователя содержит недопустимые символы: {username}",
                context={"username": username, "invalid_chars": invalid_chars, "function": "format_username_for_linux"}
            )
            # Очищаем недопустимые символы
            cleaned_username = username
            for char in invalid_chars:
                cleaned_username = cleaned_username.replace(char, '')
            username = cleaned_username
        
        if '.' in username:
            user, domain_suffix = username.split('.', 1)
            domain = domains.get(domain_suffix.strip(), domain_suffix.strip())
        else:
            user = username
            domain = domains.get('default', 'default')

        formatted_username = f"{user}@{domain}".lower()
        logger.info(f"Имя пользователя '{username}' отформатировано как '{formatted_username}'")
        
        return formatted_username
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.USER_002,
            details=f"Неожиданная ошибка при форматировании имени пользователя: {username}",
            exception=e,
            context={"username": username, "function": "format_username_for_linux"}
        )
        return ''


def set_permissions(path, user, group_name='domain users'):
    """
    Установка прав доступа для директорий и файлов.

    :param path: Путь к директории.
    :param user: Имя пользователя.
    :param group_name: Имя группы.
    """
    try:
        if not os.path.exists(path):
            handle_migration_error(
                MigrationErrorCodes.TARGET_001,
                details=f"Путь не существует: {path}",
                context={"path": path, "user": user, "group_name": group_name, "function": "set_permissions"}
            )
            return False
        
        # Получаем UID и GID
        try:
            uid = pwd.getpwnam(user).pw_uid
        except KeyError as e:
            handle_migration_error(
                MigrationErrorCodes.USER_001,
                details=f"Пользователь не найден в системе: {user}",
                exception=e,
                context={"path": path, "user": user, "group_name": group_name, "function": "set_permissions"}
            )
            return False
        
        try:
            gid = grp.getgrnam(group_name).gr_gid
        except KeyError as e:
            handle_migration_error(
                MigrationErrorCodes.USER_001,
                details=f"Группа не найдена в системе: {group_name}",
                exception=e,
                context={"path": path, "user": user, "group_name": group_name, "function": "set_permissions"}
            )
            return False

        # Рекурсивно изменить владельца и группу
        files_processed = 0
        for root, dirs, files in os.walk(path):
            try:
                os.chown(root, uid, gid)
                files_processed += 1
                
                for directory in dirs:
                    dirpath = os.path.join(root, directory)
                    os.chown(dirpath, uid, gid)
                    files_processed += 1
                    
                for file in files:
                    filepath = os.path.join(root, file)
                    os.chown(filepath, uid, gid)
                    files_processed += 1
                    
            except PermissionError as e:
                handle_migration_error(
                    MigrationErrorCodes.TARGET_003,
                    details=f"Недостаточно прав для изменения владельца: {root}",
                    exception=e,
                    context={"path": path, "user": user, "group_name": group_name, "current_path": root, "function": "set_permissions"}
                )
                return False
                
            except OSError as e:
                handle_migration_error(
                    MigrationErrorCodes.TARGET_003,
                    details=f"Ошибка системы при изменении владельца: {root}",
                    exception=e,
                    context={"path": path, "user": user, "group_name": group_name, "current_path": root, "function": "set_permissions"}
                )
                return False

        logger.info(f'Права доступа для {path} установлены на {user}:{group_name}. Обработано объектов: {files_processed}')
        return True
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.TARGET_003,
            details=f"Неожиданная ошибка при установке прав доступа для {path}",
            exception=e,
            context={"path": path, "user": user, "group_name": group_name, "function": "set_permissions"}
        )
        return False


def copy_skel(home_dir):
    """
    Копирование скелета пользователя.
    """
    skel_dir = '/etc/skel'
    try:
        if not os.path.exists(skel_dir):
            handle_migration_error(
                MigrationErrorCodes.SOURCE_001,
                details=f"Директория скелета не найдена: {skel_dir}",
                context={"home_dir": home_dir, "skel_dir": skel_dir, "function": "copy_skel"}
            )
            return False
        
        if os.path.exists(home_dir):
            logger.info(f'Домашняя директория {home_dir} уже существует.')
            return True
        
        # Создаем родительскую директорию если она не существует
        parent_dir = os.path.dirname(home_dir)
        if not os.path.exists(parent_dir):
            try:
                os.makedirs(parent_dir, exist_ok=True)
            except PermissionError as e:
                handle_migration_error(
                    MigrationErrorCodes.TARGET_001,
                    details=f"Недостаточно прав для создания родительской директории: {parent_dir}",
                    exception=e,
                    context={"home_dir": home_dir, "skel_dir": skel_dir, "parent_dir": parent_dir, "function": "copy_skel"}
                )
                return False
        
        # Копируем скелет
        shutil.copytree(skel_dir, home_dir)
        logger.info(f'Скелет из {skel_dir} скопирован в {home_dir}.')
        return True
        
    except PermissionError as e:
        handle_migration_error(
            MigrationErrorCodes.TARGET_001,
            details=f"Недостаточно прав для копирования скелета в {home_dir}",
            exception=e,
            context={"home_dir": home_dir, "skel_dir": skel_dir, "function": "copy_skel"}
        )
        return False
        
    except OSError as e:
        handle_migration_error(
            MigrationErrorCodes.COPY_001,
            details=f"Ошибка файловой системы при копировании скелета в {home_dir}",
            exception=e,
            context={"home_dir": home_dir, "skel_dir": skel_dir, "function": "copy_skel"}
        )
        return False
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.COPY_001,
            details=f"Неожиданная ошибка при копировании скелета в {home_dir}",
            exception=e,
            context={"home_dir": home_dir, "skel_dir": skel_dir, "function": "copy_skel"}
        )
        return False
