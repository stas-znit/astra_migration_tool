import os
import logging
import shutil
import datetime
import threading
import time
import argparse
import yaml
import sys
from cryptography.fernet import Fernet

from src.logging.logger import setup_logger
from src.connection.dfs_connector import mount_dfs, umount_dfs
from src.connection.usb_connector import mount_usb, umount_usb
from src.ntfs.ntfs_mounter import handle_additional_disks
from src.config.config_loader import load_config, save_config, encrypt_all_config, encrypt_only_password
from src.shortcuts_printers.links_handler import parse_links_file
from src.shortcuts_printers.shortcut_creator import create_shortcuts
from src.shortcuts_printers.printer_connector import connect_printers
from src.migration.direct_migration import direct_migrate, resume_direct_migration
from src.migration.state_tracker import load_state, update_global_state, update_user_state
from src.structure.structure_normalizer import get_users_from_host_dir, format_username_for_linux, set_permissions, copy_skel
from src.config.config_loader import fill_placeholders
from src.metrics_monitoring.report import generate_report
from src.metrics_monitoring.report_utils import calculate_additional_report_data
from src.notify.notify import send_status
from src.notify.heartbeat import Heartbeat

# Настройка логгера
logger = logging.getLogger(__name__)

# Функция для обновления last_heartbeat
def heartbeat_thread(stop_event, interval=30):
    # Поток для обновления last_heartbeat
    while not stop_event.is_set():
        update_global_state(last_heartbeat=datetime.datetime.now().isoformat())
        time.sleep(interval)

def main():
    """
    Основная функция для выполнения миграции данных с сетевого хранилища на локальную машину.
    """
    setup_logger()
    logger = logging.getLogger(__name__)

    # ------------------- Парсер аргументов -------------------
    parser = argparse.ArgumentParser(description="Скрипт миграции данных + шифрование конфига.")
    parser.add_argument("--encrypt-all", action="store_true",
                        help="Зашифровать целиком конфиг-файл (#ALL).")
    parser.add_argument("--encrypt-pass", action="store_true",
                        help="Зашифровать только поле 'password' (#PWD).")
    parser.add_argument("--config-yaml", default="src/config/settings.yaml",
                        help="Путь к конфигурационному файлу (по умолчанию src/config/settings.yaml).")

    args = parser.parse_args()

    # 3. Сценарий шифрования
    if args.encrypt_all:
        encrypt_all_config(args.config_yaml)
        logger.info("Шифрование всего файла завершено. Выход.")
        sys.exit(0)

    if args.encrypt_pass:
        encrypt_only_password(args.config_yaml)
        logger.info("Шифрование поля password завершено. Выход.")
        sys.exit(0)

    # 4. Иначе - обычный сценарий миграции
    config = load_config(args.config_yaml)
    heartbeat = Heartbeat()
    heartbeat.send_heartbeat("started", "global")
    try:
        # Файл для сохранения несовпадений хэш-сумм
        mismatch_file = config["HASH_MISMATCH_FILE"]

        # Определение типа источника данных
        data_source_type = config["DATA_SOURCE_TYPE"].lower()

        if data_source_type == 'network':
            # Подключение к сетевому хранилищу
            mount_point = mount_dfs()
            source_folder = os.path.join(mount_point, config["SOURCE_FOLDER"].lstrip('/'))
        elif data_source_type == 'usb':
            # Подключение USB-накопителя
            source_folder = mount_usb(config)
        else:
            logger.error(f"Неподдерживаемый тип источника данных: {data_source_type}")
            update_global_state(status="failed", last_update=datetime.datetime.now().isoformat(),
                                last_error={"code": "UNSUPPORTED_DATASOURCE", "message": f"Тип источника {data_source_type} не поддерживается"})
            heartbeat.send_heartbeat("error_UNSUPPORTED_DATASOURCE", "global")
            return
            
        # Проверяем, что исходная папка существует
        if not os.path.isdir(source_folder):
            logger.error(f"Исходная папка {source_folder} не найдена или недоступна.")
            update_global_state(status="failed", last_update=datetime.datetime.now().isoformat(),
                                last_error={"code": "SOURCE_NOT_FOUND", "message": f"Исходная папка {source_folder} недоступна"})
            heartbeat.send_heartbeat("error_SOURCE_NOT_FOUND", "global")
            return
        
        # Обработка дополнительных дисков
        additional_disk_mapping = handle_additional_disks()

        # Получение списка пользователей из исходной папки
        users = get_users_from_host_dir(source_folder, config["EXCLUDE_DIRS"])
        # Получение статуса миграции пользователей
        state = load_state()
        # Получаем количество пользователей для вычисления процентов миграции
        total_users = len(users)
        users_completed = 0

        # Устанавливаем статус миграции global
        update_global_state(status="in_progress", last_heartbeat=datetime.datetime.now().isoformat())
        heartbeat.send_heartbeat("runnig", "global")

        # Запускаем heartbeat-поток
        stop_heartbeat = threading.Event()
        hb_thread = threading.Thread(target=heartbeat_thread, args=(stop_heartbeat,30), daemon=True)
        hb_thread.start()
        

        try:
            for user in users:
                try:
                    # Формирование имени пользователя для Linux
                    linux_user = format_username_for_linux(user)
                    # Формирование пути к отчету о миграции данных
                    report_file_path = os.path.join(
                        config["REPORT_DIRECTORY"],
                        f"migration_report_{linux_user}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
                    )

                    # Проверяем, что миграция для пользователя ещё не была выполнена
                    if state.get(linux_user) == "success":
                        logger.info(f"Миграция для пользователя {linux_user} уже выполнена. Пропуск.")
                        users_completed += 1
                        continue
                        
                    # Получение пути к директории пользователя
                    user_dir = os.path.join(source_folder, user)

                    # Проверяем, что директория пользователя существует
                    if not os.path.isdir(user_dir):
                        logger.error(f"Директория пользователя не найдена: {user_dir}")
                        update_user_state(linux_user, "failed")
                        continue

                    # Инициализация report_data для пользователя
                    final_target_dir = os.path.join('/home', linux_user)
                    report_data = {
                        'username': linux_user,             # Имя пользователя
                        'source_dir': user_dir,             # Директория источник
                        'target_dir': final_target_dir,     # Целевая директория
                        'total_files': 0,                   # Общее количество файлов
                        'total_size': 0,                    # Общий размер файлов
                        'target_size': 0,                   # Размер скопированных файлов
                        'files_copied': 0,                  # Кол-во скопированных файлов
                        'copy_errors': [],                  # Ошибка при копировании
                        'renamed_files':[],                 # Переименованные файлы
                        'skipped_files': [],                # Пропущенные файлы (исключения, старые файлы)
                        'files_verified': 0,                # Файлы прошедшие проверку целостности
                        'discrepancies': [],                # Расхождения при проверке целостности
                        'total_copy_time': 0,               # Общее время копирования
                        'average_speed': None,              # Средняя скорость копирования
                        'start_time': datetime.datetime.now(),  # Время начала миграции
                        'end_time': None                    # Время окончания миграции
                    }
                    user = report_data.get('username')
                    logger.info(f"Отчёт о миграции пользователя {linux_user} будет сохранён в {report_file_path} по завершению миграции.")
                
                    # Сохранение состояния пользователя
                    update_user_state(linux_user, "in_progress")   

                    # Копирование скелета в папку пользователя
                    copy_skel(final_target_dir)
                    
                    # Проверяем, есть ли состояние предыдущей миграции
                    if state.get(linux_user) == "in_progress":
                        # Пытаемся возобновить миграцию
                        logger.info(f"Обнаружена незавершенная миграция для пользователя {linux_user}. Попытка возобновления.")
                        migration_success = resume_direct_migration(
                            source_dir=user_dir, 
                            target_dir=final_target_dir,
                            username=linux_user,
                            report_data=report_data
                        )
                    else:
                        # Запускаем новую миграцию
                        migration_success = direct_migrate(
                            source_dir=user_dir, 
                            target_dir=final_target_dir,
                            exclude_dirs=config["EXCLUDE_DIRS"],
                            exclude_files=config["EXCLUDE_FILES"],
                            username=linux_user,
                            report_data=report_data
                        )
                    
                    # Обработка ярлыков после миграции
                    shortcuts_file = config["SHORTCUTS_FILE_NAME"]
                    links_file_path = os.path.join(user_dir, shortcuts_file)

                    if os.path.exists(links_file_path):
                        # Парсим файл ярлыков из исходной директории пользователя
                        shortcuts = parse_links_file(user_dir)
                        # Путь к рабочему столу пользователя (уже должен быть после переименования)
                        desktop_dir = os.path.join(final_target_dir, 'Desktops', 'Desktop1')
                        if not os.path.exists(desktop_dir):
                            os.makedirs(desktop_dir)
                            logger.info(f'Создана директория рабочего стола для пользователя {linux_user}.')
                        # Создаем ярлыки на рабочем столе пользователя
                        create_shortcuts(shortcuts, desktop_dir, linux_user, additional_disk_mapping=additional_disk_mapping)
                        logger.info(f'Созданы ярлыки для {linux_user} на рабочем столе.')
                    else:
                        logger.info(f'Файл ярлыков {shortcuts_file} не найден для пользователя {linux_user}.')

                    # Установка прав доступа на целевую директорию
                    set_permissions(final_target_dir, linux_user)
                
                    # Сохранение состояния миграции
                    if migration_success:
                        update_user_state(linux_user, "success")
                    else:
                        update_user_state(linux_user, "completed_with_error")
                        # Сохраняем список несоответствий для отчета
                        if report_data.get('discrepancies'):
                            with open(mismatch_file, 'w', encoding='utf-8') as f:
                                for item in report_data['discrepancies']:
                                    f.write(f"{item}\n")
                            logger.warning(f"Миграция пользователя {linux_user} завершена с несоответствиями. Список сохранен в {mismatch_file}")

                    report_data['end_time'] = datetime.datetime.now()
                    # Рассчитываем дополнительную информацию для отчёта
                    calculate_additional_report_data(report_data)
                    # Генерация отчета
                    generate_report(report_data, report_file_path)

                    # Отправка отчета в мониторинг
                    user_report = heartbeat.create_user_report(
                        username=report_data['username'],
                        source_dir=report_data['source_dir'],
                        target_dir=report_data['target_dir'],
                        total_files=report_data['total_files'],
                        total_size=str(report_data['total_size']),
                        target_size=str(report_data['target_size']),
                        files_copied=report_data['files_copied'],
                        copy_errors=report_data['copy_errors'],
                        files_verified=report_data['files_verified'],
                        discrepancies=report_data['discrepancies'],
                        start_time=report_data['start_time'],
                        end_time=report_data['end_time']
                    )
                    heartbeat.send_report(user_report)

                    logger.info(f"Отчёт о миграции пользователя {linux_user} сохранён в {report_file_path}.")

                    # Вычисляем процент миграции
                    users_completed += 1
                    overall_progress = (users_completed / total_users) * 100
                    # Отправляем статус миграции в GUI
                    send_status(
                        progress=overall_progress,
                        status=f"Миграция пользователя {linux_user} завершена",
                        user=linux_user,
                        stage="Завершение пользователя",
                        data_volume=f"{report_data['target_size'] / (1024 * 1024):.2f} MB",
                        eta="Рассчитывается..."
                    )
                except Exception as e:
                    logger.exception(f"Ошибка при миграции данных для пользователя {linux_user}: {e}")
                    update_global_state(status="failed", last_update=datetime.datetime.now().isoformat(),
                                            last_error={"code": "USER_MIGRATION_ERROR", "message": str(e)})
                    heartbeat.send_heartbeat("error_USER_MIGRATION_ERROR", "global")
                    update_user_state(linux_user, "failed")

            # Подключение сетевых принтеров
            logger.info('Подключение сетевых принтеров...')
            printers_file = config["PRINTERS_FILE_LIST"]
            if os.path.exists(printers_file):
                connect_printers(printers_file)
                logger.info('Сетевые принтеры подключены.')
            # Отправляем статус миграции в GUI
            send_status(
                progress=100,
                status="Миграция всех пользователей завершена",
                user="Все пользователи",
                stage="Завершение",
                data_volume="Все данные перенесены",
                eta="0:00:00"
            )
            update_global_state(status="success", last_update=datetime.datetime.now().isoformat())
            heartbeat.send_heartbeat("completed", "global")

        except Exception as e:
            logger.exception(f"Ошибка выполнения миграции: {e}")
            update_global_state(status="failed", last_update=datetime.datetime.now().isoformat(),
                                    last_error={"code": "GLOBAL_MIGRATION_ERROR", "message": str(e)})
            heartbeat.send_heartbeat("error_GLOBAL_MIGRATION_ERROR", "global")
        finally:
            # Останавливаем heartbeat
            stop_heartbeat.set()
            hb_thread.join()

            if data_source_type == 'network':
                umount_dfs()
                logger.info("Сетевое хранилище отмонтировано.")
            elif data_source_type == 'usb':
                umount_usb(config)
                logger.info("USB-накопитель отмонтирован.")

    except Exception as e:
        logger.exception(f"Ошибка при инициализации миграции: {e}")
        update_global_state(status="failed", last_update=datetime.datetime.now().isoformat(),
                            last_error={"code": "INIT_ERROR", "message": str(e)})
        heartbeat.send_heartbeat("error_INIT_ERROR", "global")

if __name__ == "__main__":
    main()
