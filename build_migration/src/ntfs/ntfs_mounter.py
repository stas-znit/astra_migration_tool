"""
Модуль монтирования дополнительных разделов жесткого диска
"""

import subprocess
import logging
import os
from src.config.config_loader import load_config

logger = logging.getLogger(__name__)
config = load_config()

def find_additional_disks():
    """
    Поиск дополнительных жёстких дисков
    """
    try:
        output = subprocess.check_output(['lsblk', '-o', 'NAME,TYPE', '-nr']).decode()
        disks = []
        for line in output.strip().split('\n'):
            name, type_ = line.strip().split()
            if type_ == 'disk':
                disks.append(name)
        # Предполагаем, что основной диск - sda
        if 'sda' in disks:
            disks.remove('sda')
        if disks:
            return disks  # Возвращаем список дополнительных дисков
        else:
            logger.info("Дополнительные жёсткие диски не найдены.")
            return []
    except Exception as e:
        logger.error(f"Ошибка при поиске дополнительных дисков: {e}")
        return []
    

def assign_drive_letters(disks):
    """
    Назначение букв разделам диска
    """
    drive_letters = []
    start_ord = ord('D')
    for i, disk in enumerate(disks):
        letter = chr(start_ord + i)
        drive_letters.append(letter)
    return drive_letters


def get_partition_uuid(partition):
    """
    Получение UUID раздела
    """
    try:
        output = subprocess.check_output(['blkid', partition]).decode().strip()
        for part in output.split():
            if 'UUID=' in part:
                uuid = part.split('=')[1].strip('"')
                return uuid
        logger.error(f"UUID для раздела {partition} не найден.")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при получении UUID для {partition}: {e}")
        return None


def create_mount_point(mount_point):
    """
    Создание точки монтирования дополнительного диска
    """
    if not os.path.exists(mount_point):
        os.makedirs(mount_point)
        logger.info(f"Создана точка монтирования: {mount_point}")
    else:
        logger.info(f"Точка монтирования {mount_point} уже существует.")


def add_fstab_entry(uuid, mount_point, file_system='auto', options='defaults'):
    """
    Добавление записи в /etc/fstab
    """
    fstab_entry = f"UUID={uuid} {mount_point} {file_system} {options} 0 0\n"
    with open('/etc/fstab', 'a') as fstab:
        fstab.write(fstab_entry)
    logger.info(f"Добавлена запись в /etc/fstab: {fstab_entry.strip()}")



def mount_disk(mount_point):
    """
    Монтирование диска
    """
    try:
        subprocess.check_call(['mount', mount_point])
        logger.info(f"Диск смонтирован в {mount_point}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при монтировании диска в {mount_point}: {e}")


def is_disk_mounted(mount_point):
    """
    Проверка, смонтирован ли диск
    """
    return os.path.ismount(mount_point)


def handle_additional_disks():
    """
    Обработка дополнительных дисков
    """
    disks = find_additional_disks()
    if not disks:
        logger.info("Дополнительные диски не найдены. Пропуск обработки.")
        return {}
    
    drive_letters = assign_drive_letters(disks)
    additional_disk_mapping = {}  # Словарь для сопоставления букв дисков и точек монтирования

    for disk_name, letter in zip(disks, drive_letters):
        partition = f'/dev/{disk_name}1'  # Предполагаем, что используем первый раздел
        mount_point = f'/media/volume/{letter}'
        create_mount_point(mount_point)

        if not is_disk_mounted(mount_point):
            uuid = get_partition_uuid(partition)
            if not uuid:
                logger.error(f"Не удалось получить UUID для диска {disk_name}. Пропуск.")
                continue
            add_fstab_entry(uuid, mount_point)
            mount_disk(mount_point)
        else:
            logger.info(f"Диск {disk_name} уже смонтирован в {mount_point}")

        # Добавляем в mapping
        additional_disk_mapping[letter] = mount_point

    return additional_disk_mapping  # Возвращаем mapping для использования в других функциях
