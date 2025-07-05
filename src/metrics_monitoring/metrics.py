"""
Модуль для работы с метриками.

Функции:
    - track_migration_start: Инициализирует метрики перед началом миграции.
    - track_file_migrated: Увеличивает счетчик успешно мигрированных файлов.
    - track_file_failed: Увеличивает счетчик неудачных миграций файлов.
    - track_migration_time: Отмечает время завершения миграции.
    - update_migration_speed: Обновляет метрику скорости миграции.
    - start_metrics_server: Запускает HTTP сервер для экспорта метрик Prometheus.
"""
import os
import shutil

import time
from prometheus_client import start_http_server, Summary, Counter, Gauge

# Создание метрик
FILES_TOTAL = Gauge('files_total', 'Общее количество файлов для миграции')
FILES_MIGRATED = Counter('files_migrated', 'Количество успешно мигрированных файлов')
FILES_FAILED = Counter('files_failed', 'Количество файлов, которые не удалось мигрировать')
MIGRATION_TIME = Summary('migration_time', 'Время миграции')
CURRENT_SPEED = Gauge('current_migration_speed', 'Текущая скорость миграции в файлах в секунду')

def start_metrics_server(port=8000):
    """
    Запускает HTTP сервер для экспорта метрик Prometheus.

    :param port: Порт, на котором будет запущен сервер. По умолчанию 8000.
    """
    start_http_server(port)

def track_migration_start(total_files):
    """
    Инициализирует метрики перед началом миграции.

    :param total_files: Общее количество файлов для миграции.
    """
    FILES_TOTAL.set(total_files)

def track_file_migrated():
    """
    Увеличивает счетчик успешно мигрированных файлов.
    """
    FILES_MIGRATED.inc()

def track_file_failed():
    """
    Увеличивает счетчик неудачных миграций файлов.
    """
    FILES_FAILED.inc()

def update_migration_speed(start_time):
    """
    Обновляет метрику скорости миграции.

    :param start_time: Время начала миграции.
    """
    elapsed_time = time.time() - start_time
    CURRENT_SPEED.set(FILES_MIGRATED._value.get() / elapsed_time)

def track_migration_time(start_time):
    """
    Отмечает время завершения миграции.

    :param start_time: Время начала миграции.
    """
    MIGRATION_TIME.observe(time.time() - start_time)
