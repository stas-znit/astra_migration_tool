"""
Модуль для монтирования DFS хранилища на основе конфигурационных данных.

Функции:
    - mount_dfs: Монтирует DFS директорию с использованием CIFS и возвращает точку монтирования.
        :param max_attempts: максимальное количество попыток
        :param delay_between_attempts: пауза между попытками
    - umount_dfs: Размонтирует DFS директорию
"""

import os
import shutil
import subprocess
import logging
import time

from src.config.config_loader import load_config, get_hostname

logger = logging.getLogger(__name__)

# Получение конфигурации
config = load_config()
# Поулчаем имя компьютера
hostname = get_hostname()

# Извлекаем нужные поля
dfs_path = os.path.normpath(config["CONNECTION"]["host"])
mount_point = os.path.normpath(config["MOUNT_POINT"])
login = config["CONNECTION"]["username"]

# Предположим, пароль уже расшифрован и лежит либо в
# config.CONNECTION["password"], либо в config.CONNECTION["ENC_PASSWORD"] (уже открытая строка).
# Для примера используем "password".
password = config["CONNECTION"].get("password") or config["CONNECTION"].get("ENC_PASSWORD")

def mount_dfs(max_attempts=None, delay_between_attempts=None):
    """
    Монтирование DFS директории (CIFS).
    Если max_attempts или delay_between_attempts не указаны,
    берём их из config (MOUNT_ATTEMPTS, MOUNT_DELAY_TIME).

    :param max_attempts: Максимальное количество попыток монтирования
    :param delay_between_attempts: Пауза между попытками
    :return: Путь mount_point, если успешно
    """
    if max_attempts is None:
        max_attempts = config["MOUNT_ATTEMPTS"]
    if delay_between_attempts is None:
        delay_between_attempts = config["MOUNT_DELAY_TIME"]

    cred_file_path = None

    try:
        # Проверка команды mount
        if shutil.which("mount") is None:
            logger.error("Команда 'mount' не найдена.")
            raise FileNotFoundError("Команда 'mount' не найдена.")

        # Проверка команды umount
        if shutil.which("umount") is None:
            logger.error("Команда 'umount' не найдена.")
            raise FileNotFoundError("Команда 'umount' не найдена.")

        # Создаём точку, если нет
        if not os.path.exists(mount_point):
            os.makedirs(mount_point, mode=0o700)

        # Проверяем, не смонтирован ли ресурс
        if os.path.ismount(mount_point):
            with open('/proc/mounts', 'r') as mounts:
                for line in mounts:
                    if mount_point in line and dfs_path in line:
                        logger.info(f"Сетевое хранилище {dfs_path} уже монтировано в {mount_point}.")
                        return mount_point
            # Иначе размонтируем и очищаем
            subprocess.run(['umount', mount_point], check=True)
            shutil.rmtree(mount_point)
            os.makedirs(mount_point, mode=0o700)

        # Создание файла с кредами
        cred_file_path = os.path.normpath(config["CONNECTION"]["cred_file"])
        with open(cred_file_path, 'w') as cred_file:
            cred_file.write(f"username={login}\n")
            cred_file.write(f"password={password}\n")
        os.chmod(cred_file_path, 0o600)

        # Формируем команду монтирования
        #cmd = [
        #    'mount', '-t', 'cifs', dfs_path, mount_point, '-o',
        #    f'credentials={cred_file_path},iocharset=utf8,file_mode=0700,dir_mode=0700'
        #]
        cmd = [
            'mount', '-t', 'cifs', '-o', f'credentials={cred_file_path},iocharset=utf8,file_mode=0700,dir_mode=0700', dfs_path, mount_point
        ]

        # Попытки
        attempt = 1
        while attempt <= max_attempts:
            try:
                logger.info(f"Монтирование: {mount_point} <- {dfs_path}")
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, timeout=15)
                logger.info(f"DFS {dfs_path} смонтировано в {mount_point}.")
                return mount_point
            except subprocess.CalledProcessError as e:
                logger.error(f"Ошибка монтирования ({attempt}/{max_attempts}): {e.stderr}")
                if attempt == max_attempts:
                    logger.error("Достигнут лимит попыток.")
                    raise
                else:
                    attempt += 1
                    time.sleep(delay_between_attempts)

    except Exception as e:
        logger.error(f"Неизвестная ошибка при монтировании: {e}")
        raise
    finally:
        # Удаляем cred_file (если создан)
        if cred_file_path and os.path.exists(cred_file_path):
            try:
                if shutil.which("shred"):
                    logger.info(f"Используем shred для более надёжного удаления {cred_file_path}.")
                    subprocess.run(["shred", "--remove", cred_file_path], check=True)
                else:
                    logger.warning(f"shred не найден, удаляем обычным способом {cred_file_path}.")
                    os.remove(cred_file_path)
            except Exception as e:
                logger.warning(f"Не удалось удалить файл кредов: {e}")
                

def umount_dfs():
    """
    Размонтирование сетевого хранилища
    """
    try:
        if os.path.ismount(mount_point):
            subprocess.run(['umount', '-l', mount_point], check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
            logger.info(f'Сетевое хранилище размонтировано: {mount_point}')
        else:
            logger.info(f'Сетевое хранилище не смонтировано: {mount_point}')

    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка размонтирования {dfs_path}: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Неизвестная ошибка при размонтировании: {e}")
        raise
