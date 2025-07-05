"""
Модуль для монтирования DFS хранилища на основе конфигурационных данных.

Функции:
    - mount_dfs: Монтирует DFS директорию с использованием CIFS и возвращает точку монтирования.
        :param max_attempts: максимальное количество попыток
        :param delay_between_attempts: пауза между попытками
    - umount_dfs: Размонтирует DFS директорию

ИНТЕГРАЦИЯ КОДОВ ОШИБОК: Добавлена стандартизированная обработка ошибок
для всех операций монтирования с сохранением существующей функциональности.
"""

import os
import shutil
import subprocess
import logging
import time

from src.config.config_loader import load_config, get_hostname

from src.errors.error_codes import MigrationErrorCodes
from src.migration.state_tracker import handle_migration_error

logger = logging.getLogger(__name__)

# Получение конфигурации
config = load_config()
# Получаем имя компьютера
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
        if shutil.which("mount") is None:
            handle_migration_error(
                MigrationErrorCodes.SYSTEM_002,
                details="Команда 'mount' не найдена в системе",
                context={"command": "mount", "hostname": hostname, "dfs_path": dfs_path}
            )
            logger.error("Команда 'mount' не найдена.")
            raise FileNotFoundError("Команда 'mount' не найдена.")

        if shutil.which("umount") is None:
            handle_migration_error(
                MigrationErrorCodes.SYSTEM_002,
                details="Команда 'umount' не найдена в системе",
                context={"command": "umount", "hostname": hostname, "dfs_path": dfs_path}
            )
            logger.error("Команда 'umount' не найдена.")
            raise FileNotFoundError("Команда 'umount' не найдена.")

        if not os.path.exists(mount_point):
            try:
                os.makedirs(mount_point, mode=0o700)
                logger.info(f"Создана точка монтирования: {mount_point}")
            except PermissionError as e:
                handle_migration_error(
                    MigrationErrorCodes.TARGET_003,
                    details=f"Нет прав для создания точки монтирования: {mount_point}",
                    exception=e,
                    context={"mount_point": mount_point, "hostname": hostname, "dfs_path": dfs_path}
                )
                raise
            except OSError as e:
                if "No space left" in str(e):
                    handle_migration_error(
                        MigrationErrorCodes.TARGET_002,
                        details=f"Недостаточно места для создания точки монтирования",
                        exception=e,
                        context={"mount_point": mount_point, "hostname": hostname}
                    )
                else:
                    handle_migration_error(
                        MigrationErrorCodes.TARGET_001,
                        details=f"Не удалось создать точку монтирования: {mount_point}",
                        exception=e,
                        context={"mount_point": mount_point, "hostname": hostname}
                    )
                raise

        if os.path.ismount(mount_point):
            try:
                with open('/proc/mounts', 'r') as mounts:
                    for line in mounts:
                        if mount_point in line and dfs_path in line:
                            logger.info(f"Сетевое хранилище {dfs_path} уже смонтировано в {mount_point}.")
                            return mount_point
                
                # Иначе размонтируем и очищаем
                logger.info(f"Точка монтирования {mount_point} занята другим ресурсом, очищаем...")
                try:
                    subprocess.run(['umount', mount_point], check=True, timeout=10)
                except subprocess.CalledProcessError as e:
                    handle_migration_error(
                        MigrationErrorCodes.MOUNT_003,
                        details=f"Не удалось размонтировать занятую точку монтирования",
                        exception=e,
                        context={"mount_point": mount_point, "command_output": e.stderr}
                    )
                    # Пытаемся принудительно
                    subprocess.run(['umount', '-f', mount_point], check=True, timeout=10)
                
                shutil.rmtree(mount_point)
                os.makedirs(mount_point, mode=0o700)
                
            except FileNotFoundError as e:
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_002,
                    details="Файл /proc/mounts недоступен",
                    exception=e,
                    context={"hostname": hostname, "mount_point": mount_point}
                )
                # Продолжаем работу, но с предупреждением
                logger.warning("Не удалось проверить /proc/mounts, продолжаем монтирование")
            except Exception as e:
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_002,
                    details="Ошибка при проверке состояния монтирования",
                    exception=e,
                    context={"mount_point": mount_point, "hostname": hostname}
                )
                # Продолжаем работу

        #  Создание файла с кредами с обработкой ошибок ***
        cred_file_path = os.path.normpath(config["CONNECTION"]["cred_file"])
        
        # Проверяем наличие пароля
        if not password:
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Пароль для подключения к DFS не найден в конфигурации",
                context={"dfs_path": dfs_path, "username": login, "config_fields": list(config["CONNECTION"].keys())}
            )
            raise ValueError("Пароль для подключения к DFS не найден")
        
        try:
            # Создаем директорию для файла кредов, если необходимо
            cred_dir = os.path.dirname(cred_file_path)
            if not os.path.exists(cred_dir):
                os.makedirs(cred_dir, mode=0o700)
            
            with open(cred_file_path, 'w') as cred_file:
                cred_file.write(f"username={login}\n")
                cred_file.write(f"password={password}\n")
            os.chmod(cred_file_path, 0o600)
            logger.debug(f"Файл кредов создан: {cred_file_path}")
            
        except PermissionError as e:
            handle_migration_error(
                MigrationErrorCodes.TARGET_003,
                details=f"Нет прав для создания файла кредов: {cred_file_path}",
                exception=e,
                context={"cred_file": cred_file_path, "username": login, "hostname": hostname}
            )
            raise
        except OSError as e:
            if "No space left" in str(e):
                handle_migration_error(
                    MigrationErrorCodes.TARGET_002,
                    details="Недостаточно места для создания файла кредов",
                    exception=e,
                    context={"cred_file": cred_file_path}
                )
            else:
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_002,
                    details=f"Системная ошибка при создании файла кредов",
                    exception=e,
                    context={"cred_file": cred_file_path}
                )
            raise

        #  Формирование команды монтирования с проверкой параметров ***
        try:
            # Проверяем корректность путей
            if not dfs_path or not mount_point:
                handle_migration_error(
                    MigrationErrorCodes.CONFIG_001,
                    details="Некорректные параметры для монтирования DFS",
                    context={"dfs_path": dfs_path, "mount_point": mount_point, "hostname": hostname}
                )
                raise ValueError(f"Некорректные параметры: dfs_path={dfs_path}, mount_point={mount_point}")
            
            # Нормализуем пути для избежания проблем с пробелами и спецсимволами
            dfs_path_normalized = os.path.normpath(dfs_path)
            mount_point_normalized = os.path.normpath(mount_point)
            
            cmd = [
                'mount', '-t', 'cifs', '-o', 
                f'credentials={cred_file_path},iocharset=utf8,file_mode=0700,dir_mode=0700',
                dfs_path_normalized, 
                mount_point_normalized
            ]
            
            logger.debug(f"Команда монтирования: {' '.join(cmd)}")
            
        except Exception as e:
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Ошибка при формировании команды монтирования",
                exception=e,
                context={"dfs_path": dfs_path, "mount_point": mount_point, "cred_file": cred_file_path}
            )
            raise

        # Попытки монтирования с детальной обработкой ошибок ***
        attempt = 1
        last_error = None
        
        while attempt <= max_attempts:
            try:
                logger.info(f"Монтирование ({attempt}/{max_attempts}): {mount_point} <- {dfs_path}")
                
                result = subprocess.run(
                    cmd, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    text=True, 
                    timeout=15
                )
                
                # Проверяем успешность монтирования
                if os.path.ismount(mount_point):
                    logger.info(f"DFS {dfs_path} успешно смонтировано в {mount_point}.")
                    return mount_point
                else:
                    handle_migration_error(
                        MigrationErrorCodes.MOUNT_001,
                        details=f"Команда mount выполнена без ошибок, но ресурс не смонтирован",
                        context={
                            "attempt": attempt,
                            "dfs_path": dfs_path,
                            "mount_point": mount_point,
                            "command_output": result.stdout
                        }
                    )
                    raise RuntimeError("Ресурс не смонтирован после выполнения команды mount")
                
            except subprocess.TimeoutExpired as e:
                error_msg = f"Таймаут при монтировании DFS (попытка {attempt}/{max_attempts})"
                logger.error(error_msg)
                
                handle_migration_error(
                    MigrationErrorCodes.NETWORK_002,
                    details=error_msg,
                    exception=e,
                    context={
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "dfs_path": dfs_path,
                        "timeout": 15
                    }
                )
                last_error = e
                
            except subprocess.CalledProcessError as e:
                error_output = e.stderr.strip() if e.stderr else ""
                error_msg = f"Ошибка монтирования ({attempt}/{max_attempts}): {error_output}"
                logger.error(error_msg)
                
                # Анализируем тип ошибки по выводу stderr
                if "Permission denied" in error_output or "access denied" in error_output.lower():
                    handle_migration_error(
                        MigrationErrorCodes.NETWORK_003,
                        details=f"Ошибка аутентификации при подключении к DFS",
                        exception=e,
                        context={
                            "attempt": attempt,
                            "dfs_path": dfs_path,
                            "username": login,
                            "error_output": error_output
                        }
                    )
                elif "Network is unreachable" in error_output or "No route to host" in error_output:
                    handle_migration_error(
                        MigrationErrorCodes.NETWORK_001,
                        details=f"Сетевой ресурс недоступен",
                        exception=e,
                        context={
                            "attempt": attempt,
                            "dfs_path": dfs_path,
                            "hostname": hostname,
                            "error_output": error_output
                        }
                    )
                elif "Connection timed out" in error_output or "Connection refused" in error_output:
                    handle_migration_error(
                        MigrationErrorCodes.NETWORK_002,
                        details=f"Проблемы с сетевым подключением",
                        exception=e,
                        context={
                            "attempt": attempt,
                            "dfs_path": dfs_path,
                            "error_output": error_output
                        }
                    )
                elif "bad option" in error_output.lower() or "invalid argument" in error_output.lower():
                    handle_migration_error(
                        MigrationErrorCodes.CONFIG_001,
                        details=f"Некорректные параметры команды mount",
                        exception=e,
                        context={
                            "mount_command": ' '.join(cmd),
                            "error_output": error_output,
                            "mount_point": mount_point
                        }
                    )
                elif "busy" in error_output.lower() or "in use" in error_output.lower():
                    handle_migration_error(
                        MigrationErrorCodes.TARGET_001,
                        details=f"Точка монтирования занята",
                        exception=e,
                        context={
                            "mount_point": mount_point,
                            "error_output": error_output
                        }
                    )
                else:
                    # Общая ошибка монтирования
                    handle_migration_error(
                        MigrationErrorCodes.MOUNT_001,
                        details=f"Ошибка монтирования DFS: {error_output}",
                        exception=e,
                        context={
                            "attempt": attempt,
                            "dfs_path": dfs_path,
                            "mount_point": mount_point,
                            "error_output": error_output,
                            "return_code": e.returncode
                        }
                    )
                
                last_error = e
                
            except Exception as e:
                error_msg = f"Неожиданная ошибка при монтировании (попытка {attempt}/{max_attempts}): {e}"
                logger.error(error_msg)
                
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_003,
                    details=error_msg,
                    exception=e,
                    context={
                        "attempt": attempt,
                        "dfs_path": dfs_path,
                        "mount_point": mount_point
                    }
                )
                last_error = e
            
            # Проверяем, нужна ли следующая попытка
            if attempt == max_attempts:
                handle_migration_error(
                    MigrationErrorCodes.MOUNT_003,
                    details=f"Достигнут лимит попыток монтирования DFS ({max_attempts})",
                    exception=last_error,
                    context={
                        "max_attempts": max_attempts,
                        "dfs_path": dfs_path,
                        "mount_point": mount_point,
                        "last_attempt": attempt
                    }
                )
                logger.error("Достигнут лимит попыток.")
                raise last_error or RuntimeError("Не удалось смонтировать DFS после всех попыток")
            else:
                logger.info(f"Пауза {delay_between_attempts} сек. перед следующей попыткой...")
                attempt += 1
                time.sleep(delay_between_attempts)

    except Exception as e:
        if not any(isinstance(e, exc_type) for exc_type in [subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError, PermissionError, OSError]):
            handle_migration_error(
                MigrationErrorCodes.MOUNT_001,
                details=f"Критическая ошибка при монтировании DFS",
                exception=e,
                context={
                    "dfs_path": dfs_path,
                    "mount_point": mount_point,
                    "hostname": hostname,
                    "function": "mount_dfs"
                }
            )
        logger.error(f"Неизвестная ошибка при монтировании: {e}")
        raise
    finally:
        # Безопасное удаление файла кредов с обработкой ошибок ***
        if cred_file_path and os.path.exists(cred_file_path):
            try:
                if shutil.which("shred"):
                    logger.debug(f"Используем shred для надёжного удаления {cred_file_path}")
                    subprocess.run(["shred", "--remove", cred_file_path], check=True, timeout=10)
                else:
                    logger.debug(f"shred не найден, удаляем обычным способом {cred_file_path}")
                    os.remove(cred_file_path)
                logger.debug("Файл кредов безопасно удален")
            except subprocess.CalledProcessError as e:
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_002,
                    details=f"Не удалось безопасно удалить файл кредов с помощью shred",
                    exception=e,
                    context={"cred_file": cred_file_path}
                )
                # Пытаемся удалить обычным способом
                try:
                    os.remove(cred_file_path)
                except Exception as cleanup_e:
                    logger.warning(f"Не удалось удалить файл кредов: {cleanup_e}")
            except Exception as e:
                handle_migration_error(
                    MigrationErrorCodes.SYSTEM_002,
                    details=f"Ошибка при удалении файла кредов",
                    exception=e,
                    context={"cred_file": cred_file_path}
                )
                logger.warning(f"Не удалось удалить файл кредов: {e}")
                

def umount_dfs():
    """
    Размонтирование сетевого хранилища
    """
    try:
        # Проверка состояния монтирования с обработкой ошибок ***
        if not os.path.exists(mount_point):
            logger.info(f'Точка монтирования не существует: {mount_point}')
            return
            
        if os.path.ismount(mount_point):
            try:
                logger.info(f"Размонтирование сетевого хранилища: {mount_point}")
                
                result = subprocess.run(
                    ['umount', '-l', mount_point], 
                    check=True,
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    text=True, 
                    timeout=15
                )
                
                # Проверяем успешность размонтирования
                if not os.path.ismount(mount_point):
                    logger.info(f'Сетевое хранилище успешно размонтировано: {mount_point}')
                else:
                    handle_migration_error(
                        MigrationErrorCodes.MOUNT_003,
                        details=f"Команда umount выполнена, но ресурс не размонтирован",
                        context={
                            "mount_point": mount_point,
                            "dfs_path": dfs_path,
                            "command_output": result.stdout
                        }
                    )
                    # Пытаемся принудительное размонтирование
                    logger.warning("Попытка принудительного размонтирования...")
                    subprocess.run(['umount', '-f', mount_point], check=True, timeout=10)
                    
            except subprocess.TimeoutExpired as e:
                handle_migration_error(
                    MigrationErrorCodes.NETWORK_002,
                    details=f"Таймаут при размонтировании DFS",
                    exception=e,
                    context={"mount_point": mount_point, "dfs_path": dfs_path, "timeout": 15}
                )
                logger.error(f"Таймаут при размонтировании {mount_point}")
                # Пытаемся принудительное размонтирование
                try:
                    subprocess.run(['umount', '-f', mount_point], check=True, timeout=5)
                    logger.info("Принудительное размонтирование выполнено")
                except Exception as force_e:
                    logger.error(f"Принудительное размонтирование не удалось: {force_e}")
                    raise
                    
        else:
            logger.info(f'Сетевое хранилище не было смонтировано: {mount_point}')

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.strip() if e.stderr else ""
        
        if "busy" in error_output.lower() or "in use" in error_output.lower():
            handle_migration_error(
                MigrationErrorCodes.MOUNT_003,
                details=f"Ресурс занят, невозможно размонтировать",
                exception=e,
                context={
                    "mount_point": mount_point,
                    "dfs_path": dfs_path,
                    "error_output": error_output
                }
            )
        elif "not mounted" in error_output.lower():
            # Это не ошибка, просто информация
            logger.info(f"Ресурс {mount_point} не был смонтирован")
            return
        else:
            handle_migration_error(
                MigrationErrorCodes.MOUNT_003,
                details=f"Ошибка размонтирования DFS: {error_output}",
                exception=e,
                context={
                    "mount_point": mount_point,
                    "dfs_path": dfs_path,
                    "error_output": error_output,
                    "return_code": e.returncode
                }
            )
        
        logger.error(f"Ошибка размонтирования {dfs_path}: {error_output}")
        raise
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details=f"Неожиданная ошибка при размонтировании DFS",
            exception=e,
            context={
                "mount_point": mount_point,
                "dfs_path": dfs_path,
                "hostname": hostname,
                "function": "umount_dfs"
            }
        )
        logger.error(f"Неизвестная ошибка при размонтировании: {e}")
        raise



def check_mount_status():
    """
    Проверяет текущий статус монтирования DFS.
    Возвращает словарь с детальной информацией.
    """
    try:
        status = {
            "mount_point": mount_point,
            "dfs_path": dfs_path,
            "is_mounted": False,
            "mount_info": None,
            "accessible": False,
            "error": None
        }
        
        # Проверяем, смонтировано ли
        if os.path.exists(mount_point) and os.path.ismount(mount_point):
            status["is_mounted"] = True
            
            # Получаем информацию о монтировании
            try:
                with open('/proc/mounts', 'r') as mounts:
                    for line in mounts:
                        if mount_point in line:
                            status["mount_info"] = line.strip()
                            break
            except Exception as e:
                logger.debug(f"Не удалось прочитать /proc/mounts: {e}")
            
            # Проверяем доступность
            try:
                os.listdir(mount_point)
                status["accessible"] = True
            except Exception as e:
                status["accessible"] = False
                status["error"] = str(e)
        
        return status
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_002,
            details="Ошибка при проверке статуса монтирования",
            exception=e,
            context={"mount_point": mount_point, "function": "check_mount_status"}
        )
        return {"error": str(e)}


def test_dfs_connection():
    """
    Тестирует подключение к DFS без фактического монтирования.
    Полезно для диагностики проблем.
    """
    try:
        logger.info("Тестирование подключения к DFS...")
        
        # Проверяем доступность хоста
        import socket
        
        try:
            # Извлекаем хост из пути DFS (//server/share -> server)
            if dfs_path.startswith('//'):
                host = dfs_path.split('/')[2]
            else:
                host = dfs_path.split('/')[0]
            
            # Проверяем доступность хоста через ping
            result = subprocess.run(
                ['ping', '-c', '1', '-W', '3', host],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                logger.info(f"Хост {host} доступен")
            else:
                handle_migration_error(
                    MigrationErrorCodes.NETWORK_001,
                    details=f"Хост DFS недоступен: {host}",
                    context={"host": host, "dfs_path": dfs_path, "ping_output": result.stderr}
                )
                return False
                
        except subprocess.TimeoutExpired:
            handle_migration_error(
                MigrationErrorCodes.NETWORK_002,
                details=f"Таймаут при проверке доступности хоста DFS",
                context={"host": host, "dfs_path": dfs_path}
            )
            return False
        except Exception as e:
            handle_migration_error(
                MigrationErrorCodes.NETWORK_001,
                details=f"Ошибка при проверке доступности хоста DFS",
                exception=e,
                context={"host": host, "dfs_path": dfs_path}
            )
            return False
        
        # Проверяем доступность SMB портов (445, 139)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((host, 445))
            sock.close()
            
            if result == 0:
                logger.info(f"SMB порт 445 доступен на хосте {host}")
                return True
            else:
                handle_migration_error(
                    MigrationErrorCodes.NETWORK_001,
                    details=f"SMB порт 445 недоступен на хосте {host}",
                    context={"host": host, "port": 445, "dfs_path": dfs_path}
                )
                return False
                
        except Exception as e:
            handle_migration_error(
                MigrationErrorCodes.NETWORK_001,
                details=f"Ошибка при проверке SMB портов",
                exception=e,
                context={"host": host, "dfs_path": dfs_path}
            )
            return False
            
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details="Критическая ошибка при тестировании подключения к DFS",
            exception=e,
            context={"dfs_path": dfs_path, "function": "test_dfs_connection"}
        )
        return False


def cleanup_mount_point():
    """
    Очищает точку монтирования в случае проблем.
    Полезно для восстановления после сбоев.
    """
    try:
        logger.info(f"Очистка точки монтирования: {mount_point}")
        
        # Сначала пытаемся размонтировать, если смонтировано
        if os.path.exists(mount_point) and os.path.ismount(mount_point):
            try:
                umount_dfs()
            except Exception as e:
                logger.warning(f"Не удалось размонтировать перед очисткой: {e}")
                # Принудительное размонтирование
                try:
                    subprocess.run(['umount', '-f', mount_point], check=True, timeout=10)
                except Exception as force_e:
                    handle_migration_error(
                        MigrationErrorCodes.MOUNT_003,
                        details="Не удалось принудительно размонтировать точку монтирования",
                        exception=force_e,
                        context={"mount_point": mount_point}
                    )
        
        # Удаляем и пересоздаем точку монтирования
        if os.path.exists(mount_point):
            try:
                shutil.rmtree(mount_point)
                logger.info(f"Точка монтирования удалена: {mount_point}")
            except Exception as e:
                handle_migration_error(
                    MigrationErrorCodes.TARGET_003,
                    details="Не удалось удалить точку монтирования",
                    exception=e,
                    context={"mount_point": mount_point}
                )
                raise
        
        # Пересоздаем точку монтирования
        try:
            os.makedirs(mount_point, mode=0o700)
            logger.info(f"Точка монтирования пересоздана: {mount_point}")
            return True
        except Exception as e:
            handle_migration_error(
                MigrationErrorCodes.TARGET_001,
                details="Не удалось пересоздать точку монтирования",
                exception=e,
                context={"mount_point": mount_point}
            )
            raise
            
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details="Критическая ошибка при очистке точки монтирования",
            exception=e,
            context={"mount_point": mount_point, "function": "cleanup_mount_point"}
        )
        return False


def validate_dfs_config():
    """
    Проверяет корректность конфигурации DFS.
    Возвращает список найденных проблем.
    """
    issues = []
    
    try:
        # Проверяем обязательные поля
        required_fields = {
            "CONNECTION": {
                "host": dfs_path,
                "username": login,
            },
            "MOUNT_POINT": mount_point,
            "MOUNT_ATTEMPTS": config.get("MOUNT_ATTEMPTS"),
            "MOUNT_DELAY_TIME": config.get("MOUNT_DELAY_TIME")
        }
        
        # Проверяем наличие и корректность полей
        if not dfs_path or dfs_path == "." or not dfs_path.startswith('//'):
            issues.append("Некорректный путь DFS")
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Некорректный путь DFS в конфигурации",
                context={"dfs_path": dfs_path, "expected_format": "//server/share"}
            )
        
        if not login:
            issues.append("Отсутствует имя пользователя")
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Отсутствует имя пользователя для подключения к DFS",
                context={"config_section": "CONNECTION"}
            )
        
        if not password:
            issues.append("Отсутствует пароль")
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Отсутствует пароль для подключения к DFS",
                context={"config_section": "CONNECTION", "username": login}
            )
        
        if not mount_point or mount_point == "/" or not os.path.isabs(mount_point):
            issues.append("Некорректная точка монтирования")
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details="Некорректная точка монтирования в конфигурации",
                context={"mount_point": mount_point}
            )
        
        # Проверяем числовые параметры
        try:
            attempts = int(config.get("MOUNT_ATTEMPTS", 0))
            if attempts < 1 or attempts > 10:
                issues.append("Некорректное количество попыток монтирования")
        except (ValueError, TypeError):
            issues.append("Некорректный формат MOUNT_ATTEMPTS")
        
        try:
            delay = int(config.get("MOUNT_DELAY_TIME", 0))
            if delay < 0 or delay > 60:
                issues.append("Некорректная задержка между попытками")
        except (ValueError, TypeError):
            issues.append("Некорректный формат MOUNT_DELAY_TIME")
        
        # Проверяем доступность команд
        if not shutil.which("mount"):
            issues.append("Команда 'mount' не найдена")
        
        if not shutil.which("umount"):
            issues.append("Команда 'umount' не найдена")
        
        # Если есть проблемы, логируем их
        if issues:
            handle_migration_error(
                MigrationErrorCodes.CONFIG_001,
                details=f"Найдены проблемы в конфигурации DFS: {', '.join(issues)}",
                context={
                    "issues_count": len(issues),
                    "issues": issues,
                    "dfs_path": dfs_path,
                    "mount_point": mount_point
                }
            )
        
        return issues
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details="Ошибка при проверке конфигурации DFS",
            exception=e,
            context={"function": "validate_dfs_config"}
        )
        return [f"Ошибка проверки конфигурации: {e}"]


def diagnose_mount_issues():
    """
    Выполняет комплексную диагностику проблем с монтированием DFS.
    Возвращает отчет с рекомендациями по устранению.
    """
    report = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config_issues": [],
        "network_status": "unknown",
        "mount_status": "unknown",
        "recommendations": []
    }
    
    try:
        logger.info("Запуск диагностики проблем с монтированием DFS...")
        
        # 1. Проверка конфигурации
        report["config_issues"] = validate_dfs_config()
        
        # 2. Проверка сети
        if test_dfs_connection():
            report["network_status"] = "ok"
        else:
            report["network_status"] = "failed"
            report["recommendations"].append("Проверьте сетевое подключение к серверу DFS")
        
        # 3. Проверка текущего статуса монтирования
        mount_status = check_mount_status()
        if mount_status.get("is_mounted") and mount_status.get("accessible"):
            report["mount_status"] = "ok"
        elif mount_status.get("is_mounted") and not mount_status.get("accessible"):
            report["mount_status"] = "mounted_but_inaccessible"
            report["recommendations"].append("Ресурс смонтирован, но недоступен. Попробуйте перемонтировать")
        else:
            report["mount_status"] = "not_mounted"
        
        # 4. Генерация рекомендаций
        if report["config_issues"]:
            report["recommendations"].append("Исправьте ошибки конфигурации DFS")
        
        if report["network_status"] == "failed":
            report["recommendations"].extend([
                "Проверьте доступность сервера DFS",
                "Убедитесь, что порты 445 и 139 открыты",
                "Проверьте настройки firewall"
            ])
        
        if not report["recommendations"]:
            report["recommendations"].append("Все проверки пройдены успешно")
        
        logger.info(f"Диагностика завершена. Найдено проблем: {len(report['config_issues'])}")
        return report
        
    except Exception as e:
        handle_migration_error(
            MigrationErrorCodes.SYSTEM_003,
            details="Ошибка при выполнении диагностики DFS",
            exception=e,
            context={"function": "diagnose_mount_issues"}
        )
        report["error"] = str(e)
        return report