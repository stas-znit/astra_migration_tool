import datetime
import math
from src.metrics_monitoring.report_utils import format_size

def generate_report(data, report_file_path):
    """
    Генерирует отчёт в формате Markdown.

    :param data: Словарь с данными отчёта.
    :param report_file_path: Путь для сохранения отчёта.
    :return: None
    """
    with open(report_file_path, 'w', encoding='utf-8') as f:
        f.write(f"# Отчёт о миграции данных\n\n")
        f.write(f"**Дата и время генерации отчёта:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Общая информация
        f.write(f"## Общая информация\n")
        f.write(f"- **Пользователь:** {data.get('username', 'Не указано')}\n")
        f.write(f"- **Исходная директория:** {data.get('source_dir', 'Не указано')}\n")
        f.write(f"- **Целевая директория:** {data.get('target_dir', 'Не указано')}\n")
        f.write(f"- **Время начала миграции:** {data.get('start_time_str', 'Не указано')}\n")
        f.write(f"- **Время окончания миграции:** {data.get('end_time_str', 'Не указано')}\n")
        f.write(f"- **Общее время миграции:** {data.get('total_migration_time', 'Не указано')}\n\n")

        # Объём данных
        f.write(f"## Объём данных\n")
        f.write(f"- **Общий объём данных:** {format_size(data.get('total_size', 0))}\n")
        f.write(f"- **Объём скопированных данных:** {format_size(data.get('target_size', 0))}\n")
        f.write(f"- **Средняя скорость копирования:** {data.get('average_speed', 'Неизвестно')}\n\n")

        # Результаты миграции
        f.write(f"## Результаты миграции\n")
        f.write(f"- **Всего файлов:** {data.get('total_files', 0)}\n")
        f.write(f"- **Успешно скопировано:** {data.get('files_copied', 0)}\n")
        f.write(f"- **Ошибок при копировании:** {len(data.get('copy_errors', []))}\n")
        f.write(f"- **Переименованных файлов:** {len(data.get('renamed_files', []))}\n")
        f.write(f"- **Пропущено файлов:** {len(data.get('skipped_files', []))}\n\n")

        # Ошибки при копировании
        if data.get('copy_errors'):
            f.write(f"### Ошибки при копировании файлов\n\n")
            for error in data['copy_errors']:
                f.write(f"- {error}\n")
            f.write("\n")

        # Переименованные файлы
        if data.get('renamed_files'):
            f.write(f"### Переименованные файлы\n\n")
            f.write("| Исходное имя | Новое имя |\n")
            f.write("|--------------|-----------|\n")
            for item in data['renamed_files']:
                f.write(f"| {item['original_name']} | {item['new_name']} |\n")
            f.write("\n")

        # Пропущенные файлы
        if data.get('skipped_files'):
            f.write(f"## Пропущенные файлы\n")
            f.write(f"Всего пропущенных файлов: {len(data['skipped_files'])}\n\n")
            for skipped_file in data['skipped_files']:
                f.write(f"- {skipped_file}\n")
            f.write("\n")

        # Результаты проверки целостности
        f.write(f"## Результаты проверки целостности\n")
        f.write(f"- **Файлы, прошедшие проверку:** {data.get('files_verified', 0)}\n")
        f.write(f"- **Несоответствия:** {len(data.get('discrepancies', []))}\n\n")

        # Несоответствия
        if data.get('discrepancies'):
            f.write(f"## Несоответствия при проверке целостности\n")
            f.write(f"Всего несоответствий: {len(data['discrepancies'])}\n\n")
            for discrepancy in data['discrepancies']:
                f.write(f"- {discrepancy}\n")
            f.write("\n")

        # Заключение
        f.write(f"## Заключение\n")

        copy_errors_exist = bool(data.get('copy_errors'))
        discrepancies_exist = bool(data.get('discrepancies'))

        if not copy_errors_exist and not discrepancies_exist:
            migration_result = "успешно"
            f.write(f"Миграция данных завершена **{migration_result}**.\n")
        elif copy_errors_exist and discrepancies_exist:
            migration_result = "завершена с ошибками копирования и несоответствиями при проверке целостности"
            f.write(f"Миграция данных завершена, но были обнаружены **ошибки копирования** и **несоответствия при проверке целостности данных**.\n")
        elif copy_errors_exist:
            migration_result = "завершена с ошибками копирования"
            f.write(f"Миграция данных завершена, но были обнаружены **ошибки копирования**.\n")
        elif discrepancies_exist:
            migration_result = "завершена с несоответствиями при проверке целостности"
            f.write(f"Миграция данных завершена, но были обнаружены **несоответствия при проверке целостности данных**.\n")

        f.write("\n")

        # Дополнительные рекомендации
        if copy_errors_exist or discrepancies_exist:
            f.write("**Рекомендуется**:\n")
            if copy_errors_exist:
                f.write("- Проверить список файлов с ошибками копирования в разделе **Ошибки копирования**.\n")
            if discrepancies_exist:
                f.write("- Проверить список файлов с несоответствиями в разделе **Несоответствия при проверке целостности**.\n")
            f.write("- Повторить миграцию проблемных файлов вручную или обратиться к администратору за помощью.\n")
            f.write("\n")



