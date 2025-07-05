"""
Модуль для вычисления дополнительных данных для отчета

"""

import datetime
import math


def calculate_additional_report_data(report_data):
    """
    Рассчитывает дополнительные данные для отчета.

    :param report_data: Словарь с данными отчета.
    :return: None
    """
    # Преобразуем время начала и окончания в строку
    report_data['start_time_str'] = report_data['start_time'].strftime('%Y-%m-%d %H:%M:%S')
    report_data['end_time_str'] = report_data['end_time'].strftime('%Y-%m-%d %H:%M:%S')

    # Рассчитываем общее время миграции
    total_time_sec = (report_data['end_time'] - report_data['start_time']).total_seconds()
    report_data['total_migration_time'] = str(datetime.timedelta(seconds=total_time_sec))

    # Рассчитываем среднюю скорость копирования
    if report_data.get('total_copy_time') and report_data.get('target_size'):
        avg_speed = report_data['target_size'] / report_data['total_copy_time']  # байт/сек
        avg_speed_mb = avg_speed / (1024 * 1024)  # МБ/сек
        report_data['average_speed'] = f"{avg_speed_mb:.2f} МБ/сек"
    else:
        report_data['average_speed'] = 'Неизвестно'


def format_size(size_bytes):
    """
    Форматирует размер в байтах в удобочитаемый вид.

    :param size_bytes: Размер в байтах.
    :return: Форматированный размер.
    """
    if size_bytes == 0:
        return "0 Б"
    size_name = ("Б", "КБ", "МБ", "ГБ", "ТБ")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"