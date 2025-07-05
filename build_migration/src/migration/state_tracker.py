"""
Модуль для управления состоянием миграции.

Логика:
- Храним всё в одном JSON-файле (state_file).
- Структура JSON, напр.:
  {
    "global": {
      "status": "in_progress",
      "last_update": "...",
      ...
    },
    "users": {
      "vasya": "success",
      "petya": "failed",
      ...
    }
  }

Функции:
    load_state() -> dict
    save_full_state(state_dict)
    update_global_state(**kwargs)
    update_user_state(user, status)
"""

import json
import os
import logging
import tempfile
import shutil
from src.logging.logger import setup_logger
from src.config.config_loader import load_config

setup_logger()
logger = logging.getLogger(__name__)

# Загружаем конфиг, чтобы узнать, где лежит state_file
config = load_config()
state_file = config["STATE_FILE"]  #  /mnt/.../migration_state.json

def load_state():
    """
    Загружает текущее состояние миграции из state_file.
    Если файл не найден, возвращаем структуру с {"global":{}, "users":{}}.
    """
    if not os.path.exists(state_file):
        logger.warning(f"Файл состояния {state_file} не найден. Будет применён пустой словарь.")
        return {"global": {}, "users": {}}

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Убедимся, что в data есть "global" и "users"
        if "global" not in data:
            data["global"] = {}
        if "users" not in data:
            data["users"] = {}
        logger.info("Состояние миграции успешно загружено.")
        return data
    except Exception as e:
        logger.error(f"Ошибка при загрузке состояния миграции: {e}")
        logger.warning("Будет использован пустой словарь.")
        return {"global": {}, "users": {}}

def save_full_state(state_dict):
    """
    Перезаписывает файл состояния целиком.
    Используем атомарную запись через tempfile.
    """
    state_dir = os.path.dirname(state_file)
    if not os.path.exists(state_dir):
        try:
            os.makedirs(state_dir)
        except Exception as e:
            logger.error(f"Не удалось создать директорию {state_dir}: {e}")
            return

    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=state_dir, delete=False) as tf:
            json.dump(state_dict, tf, ensure_ascii=False, indent=2)
            temp_name = tf.name
        shutil.move(temp_name, state_file)
        logger.info("Состояние миграции (полный словарь) успешно сохранено.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении полного состояния миграции: {e}")
        if temp_name and os.path.exists(temp_name):
            os.remove(temp_name)

def update_global_state(**kwargs):
    """
    Обновляет поля в state["global"].
    Пример: update_global_state(status="failed", last_error="...").
    """
    state = load_state()
    global_state = state.get("global", {})
    for k, v in kwargs.items():
        global_state[k] = v
    state["global"] = global_state

    save_full_state(state)

def update_user_state(user, status):
    """
    Устанавливает state["users"][user] = status.
    Пример: update_user_state("user1", "success").
    """
    state = load_state()
    users = state.get("users", {})
    users[user] = status
    state["users"] = users

    save_full_state(state)
