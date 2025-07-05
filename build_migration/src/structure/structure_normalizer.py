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
        users = [
            d for d in os.listdir(host_dir)
            if os.path.isdir(os.path.join(host_dir, d)) and d not in exclude_dirs
        ]
        logger.info(f"Найдено пользователей: {len(users)}")
        return users
    except FileNotFoundError as e:
        logger.error(f"Директория {host_dir} не найдена: {e}")
        return []
    except PermissionError as e:
        logger.error(f"Недостаточно прав для доступа к директории {host_dir}: {e}")
        return []
    except NotADirectoryError as e:
        logger.error(f"Директория {host_dir} не является директорией: {e}")
        return []
    except Exception as e:
        logger.error(f"Ошибка при получении списка пользователей из директории {host_dir}: {e}")
        return []

def format_username_for_linux(username: str) -> str:
    """
    Приведение имени пользователя к виду для Linux.

    :param username: Имя пользователя с доменным суффиксом.
    :return: Приведенное имя пользователя.
    """
    if not username:
        logger.error("Имя пользователя не может быть пустым")
        return ''
    
    if '.' in username:
        user, domain_suffix = username.split('.', 1)
        domain = domains.get(domain_suffix.strip(), domain_suffix.strip()) # Если домен не найден, используем сам suffix
    else:
        user = username
        domain = domains.get('default', 'default') # Если 'default' не найден, используем 'default'

    return f"{user}@{domain}".lower()


def set_permissions(path, user, group_name='domain users'):
    """
    Установка прав доступа для директорий и файлов.

    :param path: Путь к директории.
    :param user: Имя пользователя.
    :param group_name: Имя группы.
    """
    try:
        uid = pwd.getpwnam(user).pw_uid
        gid = grp.getgrnam(group_name).gr_gid

        # Рекурсивно изменить владельца и группу
        for root, dirs, files in os.walk(path):
            os.chown(root, uid, gid)
            for directory in dirs:
                dirpath = os.path.join(root, directory)
                os.chown(dirpath, uid, gid)
            for file in files:
                filepath = os.path.join(root, file)
                os.chown(filepath, uid, gid)

        logger.info(f'Права доступа для {path} установлены на {user}:{group_name}.')
    except KeyError as e:
        logger.error(f'Группа или пользователь не найдены: {e}')
    except Exception as e:
        logger.error(f'Ошибка при установке прав доступа для {path}: {e}')


def copy_skel(home_dir):
    """
    Копирование скелета пользователя.
    """
    skel_dir = '/etc/skel'
    try:
        if os.path.exists(home_dir):
            logger.info(f'Домашняя директория {home_dir} уже существует.')
        else:
            shutil.copytree(skel_dir, home_dir)
            logger.info(f'Скелет из {skel_dir} скопирован в {home_dir}.')
    except Exception as e:
        logger.error(f'Ошибка при копировании скелета в {home_dir}: {e}')
