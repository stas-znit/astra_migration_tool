"""
Модуль отправки сообщений через сокет для GUI-скрипта.
Функции:
- send_status: Отправляет сообщение GUI-скрипту через локальный сокет.

:param progress: Прогресс
:param status: Статус
:param user: Пользователь
:param stage: Стадия
:param data_volume: Объем данных
:param eta: Рассчетное время на миграцию данных
"""

import os
import socket
import logging

logger = logging.getLogger(__name__)

def send_status(progress=None, status=None, user=None, stage=None, data_volume=None, eta=None):
    """
    Отправляет сообщение GUI-скрипту через локальный сокет с обновлённой информацией.
    """
    server_address = '/tmp/migration_socket'
    if not os.path.exists(server_address):
        # Сокет не существует, GUI-скрипт не запущен
        return

    message_parts = []
    if progress is not None:
        message_parts.append(f"progress:{progress}")
    if status is not None:
        message_parts.append(f"status:{status}")
    if user is not None:
        message_parts.append(f"user:{user}")
    if stage is not None:
        message_parts.append(f"stage:{stage}")
    if data_volume is not None:
        message_parts.append(f"data_volume:{data_volume}")
    if eta is not None:
        message_parts.append(f"eta:{eta}")

    message = ';'.join(message_parts)

    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.connect(server_address)
        sock.sendall(message.encode('utf-8'))
    except FileNotFoundError:
        pass  # Игнорируем ошибку
    except Exception as e:
        logger.debug(f"Не удалось отправить сообщение GUI-скрипту: {e}")
    finally:
        sock.close()
