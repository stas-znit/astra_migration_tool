"""
Модуль для монтирования USB-накопителя на основе конфигурационных данных.

Функции:
    - mount_usb: Монтирует USB-накопитель и возвращает путь к исходной папке с данными.
    - find_usb_device: Находит USB-устройство в системе и возвращает путь к нему.
"""
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def find_usb_device():
    try:
        output = subprocess.check_output(['lsblk', '-o', 'NAME,MOUNTPOINT', '-nr']).decode()
        for line in output.strip().split('\n'):
            name, mountpoint = line.strip().split()
            if 'usb' in name.lower() and not mountpoint:
                device_path = f"/dev/{name}"
                return device_path
    except Exception as e:
        logger.error(f"Ошибка при поиске USB-устройства: {e}")
    return None


def find_usb_device_by_label(label):
    """
    Ищет USB-устройство по заданной метке (Label) и возвращает путь к его разделу.

    :param label: Метка USB-накопителя.
    :return: Путь к устройству (например, '/dev/sdb1') или None, если устройство не найдено.
    """
    import subprocess

    try:
        output = subprocess.check_output(['blkid', '-o', 'device', '-t', f'LABEL={label}']).decode().strip()
        devices = output.split('\n')
        if devices:
            return devices[0]  # Возвращаем первый найденный девайс с заданной меткой
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка при поиске USB-устройства по метке {label}: {e}")
    return None

def mount_usb(config):
    """
    Монтирует USB-накопитель и возвращает путь к исходной папке с данными.

    :param config: Конфигурационный объект.
    :return: Путь к исходной папке с данными на USB-накопителе.
    """
    usb_mount_point = config.USB_MOUNT_POINT
    usb_label = config.USB_DEVICE_LABEL
    usb_device_path = config.USB_DEVICE_PATH

    # Если указан путь к устройству, используем его
    if not usb_device_path:
        if usb_label:
            # Ищем устройство по метке
            usb_device_path = find_usb_device_by_label(usb_label)
            if not usb_device_path:
                logger.error(f"USB-устройство с меткой '{usb_label}' не найдено.")
                raise FileNotFoundError(f"USB-устройство с меткой '{usb_label}' не найдено.")
        else:
            # Используем старую функцию поиска (при необходимости)
            usb_device_path = find_usb_device()
            if not usb_device_path:
                logger.error("USB-устройство не найдено.")
                raise FileNotFoundError("USB-устройство не найдено.")

    if not os.path.exists(usb_device_path):
        logger.error(f"USB-устройство {usb_device_path} не существует.")
        raise FileNotFoundError(f"USB-устройство {usb_device_path} не существует.")

    # Проверяем, смонтирован ли USB-накопитель
    if not os.path.ismount(usb_mount_point):
        logger.info(f"Монтирование USB-накопителя {usb_device_path} в {usb_mount_point}...")
        os.makedirs(usb_mount_point, exist_ok=True)
        try:
            subprocess.check_call(['mount', usb_device_path, usb_mount_point])
            logger.info(f"USB-накопитель смонтирован в {usb_mount_point}.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Не удалось смонтировать USB-накопитель: {e}")
            raise

    # Проверяем доступность исходной папки на USB-накопителе
    source_folder = config.SOURCE_FOLDER  # Путь на USB-накопителе
    usb_source_folder = os.path.join(usb_mount_point, source_folder.lstrip('/'))
    if not os.path.isdir(usb_source_folder):
        logger.error(f"Исходная папка {usb_source_folder} не найдена на USB-накопителе.")
        raise FileNotFoundError(f"Исходная папка {usb_source_folder} не найдена.")

    return usb_source_folder


def umount_usb(config):
    """
    Отмонтирует USB-накопитель.

    :param config: Конфигурационный объект.
    """
    usb_mount_point = config.USB_MOUNT_POINT
    if os.path.ismount(usb_mount_point):
        try:
            subprocess.check_call(['umount', '-l', usb_mount_point])
            logger.info(f"USB-накопитель отмонтирован из {usb_mount_point}.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Не удалось отмонтировать USB-накопитель: {e}")
            raise
    else:
        logger.info("USB-накопитель не смонтирован.")

