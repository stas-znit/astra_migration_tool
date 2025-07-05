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
    Обработка дополнительных дисков с улучшенной обработкой ошибок.
    
    Логика работы:
    1. Находим все диски кроме основного (sda)
    2. Назначаем буквы дисков начиная с D
    3. Для каждого диска:
       - Находим разделы
       - Создаем точку монтирования
       - Получаем UUID раздела
       - Добавляем в fstab если нужно
       - Монтируем диск
    4. Возвращаем маппинг букв дисков на точки монтирования
    
    :return: Словарь {буква_диска: точка_монтирования}
    """
    disks = find_additional_disks()
    if not disks:
        logger.info("Дополнительные диски не найдены. Пропуск обработки.")
        return {}
    
    drive_letters = assign_drive_letters(disks)
    additional_disk_mapping = {}

    logger.info(f"Найдено {len(disks)} дополнительных дисков: {disks}")

    for disk_name, letter in zip(disks, drive_letters):
        try:
            logger.info(f"Обработка диска {disk_name} как {letter}:")
            
            # Ищем разделы на диске
            partitions = []
            for i in range(1, 10):  # Проверяем разделы 1-9
                partition = f'/dev/{disk_name}{i}'
                if os.path.exists(partition):
                    partitions.append(partition)
                    logger.debug(f"  Найден раздел: {partition}")
            
            if not partitions:
                logger.warning(f"  Разделы на диске {disk_name} не найдены")
                continue
            
            # Используем первый найденный раздел
            partition = partitions[0]
            mount_point = f'/media/volume/{letter}'
            
            logger.info(f"  Использую раздел: {partition}")
            logger.info(f"  Точка монтирования: {mount_point}")
            
            # Создаем точку монтирования
            create_mount_point(mount_point)

            # Проверяем, не смонтирован ли уже
            if is_disk_mounted(mount_point):
                logger.info(f"  Диск уже смонтирован в {mount_point}")
                additional_disk_mapping[letter] = mount_point
                continue

            # Получаем UUID раздела
            uuid = get_partition_uuid(partition)
            if not uuid:
                logger.error(f"  Не удалось получить UUID для раздела {partition}")
                continue
            
            logger.info(f"  UUID раздела: {uuid}")
            
            # Проверяем, нет ли уже записи в fstab
            if not _is_in_fstab(uuid):
                # Добавляем запись в fstab с правами для пользователей
                add_fstab_entry(uuid, mount_point, 'auto', 'defaults,uid=1000,gid=1000,umask=022')
                logger.info(f"  Добавлена запись в fstab")
            else:
                logger.info(f"  Запись в fstab уже существует")
            
            # Монтируем диск
            mount_disk(mount_point)
            
            # Проверяем успешность монтирования
            if is_disk_mounted(mount_point):
                additional_disk_mapping[letter] = mount_point
                logger.info(f"  ✓ Диск {disk_name} успешно смонтирован как {letter}: -> {mount_point}")
            else:
                logger.error(f"  ✗ Не удалось смонтировать диск {disk_name}")
                
        except Exception as e:
           logger.error(f"  ✗ Ошибка при обработке диска {disk_name}: {e}")
           continue

    if additional_disk_mapping:
       logger.info(f"Успешно настроено {len(additional_disk_mapping)} дополнительных дисков:")
       for letter, mount_point in additional_disk_mapping.items():
           logger.info(f"  {letter}: -> {mount_point}")
    else:
       logger.warning("Ни один дополнительный диск не был успешно смонтирован")

    return additional_disk_mapping

def _is_in_fstab(uuid: str) -> bool:
   """
   Проверка наличия UUID в /etc/fstab.
   
   :param uuid: UUID для проверки
   :return: True если UUID уже есть в fstab
   """
   try:
       with open('/etc/fstab', 'r') as f:
           content = f.read()
           return uuid in content
   except Exception as e:
       logger.warning(f"Не удалось прочитать /etc/fstab: {e}")
       return False