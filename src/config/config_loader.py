"""
Модуль для загрузки и сохранения конфигурации (YAML) в формате dict.

Основная логика:
1. Чтение YAML-файла (config.yaml) в dict.
2. Замена (шаблонизация) плейсхолдеров вида {EXTNAME}, placeholder_for_hostname и т.п.
3. Возвращение dict без обёртки в класс.
4. Сохранение dict обратно в YAML (при необходимости).
"""

import os
import yaml
from datetime import datetime
import shutil
import time
from cryptography.fernet import Fernet

# Путь к файлу конфигурации по умолчанию
CONFIG_PATH = "src/config/settings.yaml"
KEY = b'Plwg3YiebjR6-_5T8BNknwfKrFzRt-XvVhkYJb__bhw='
cipher = Fernet(KEY)

def get_hostname():
    """
    Получение имени (hostname) текущей машины без доменной части.

    :return: Строка с hostname без FQDN (например, "myhost").
    """
    return os.uname()[1].split('.')[0]

def fill_placeholders(cfg: dict):
    """
    Рекурсивно проходит по структуре данных (словарь/список) и заменяет некоторые плейсхолдеры:
      - `placeholder_for_hostname` -> реальный hostname
      - `{EXTNAME}` -> hostname
      - `{CURRENT_DATETIME}` -> текущее время "YYYY-MM-DD HH:MM:SS"
      - (Опционально) `{MOUNT_POINT}`, `{TARGET_ROOT_BUFFER_FOLDER}`, если они присутствуют.

    :param cfg: dict (результат загрузки YAML), где будет произведена замена.
    """
    hostname = get_hostname()
    current_dt = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

    # Базовые замены
    replacements = {
        "placeholder_for_hostname": hostname,
        "{EXTNAME}": hostname,
        "{CURRENT_DATETIME}": current_dt,
        "{CRED_FILE}": cfg.get("CRED_FILE", "/tmp/.smbcred_#")
    }

    def recursive_replace(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                obj[k] = recursive_replace(v)
            return obj
        elif isinstance(obj, list):
            return [recursive_replace(item) for item in obj]
        elif isinstance(obj, str):
            new_str = obj
            for old, new_val in replacements.items():
                new_str = new_str.replace(old, str(new_val))
            return new_str
        else:
            return obj

    # Первый проход
    recursive_replace(cfg)

    # Второй проход (MOUNT_POINT, TARGET_ROOT_BUFFER_FOLDER)
    if "MOUNT_POINT" in cfg and isinstance(cfg["MOUNT_POINT"], str):
        mp = cfg["MOUNT_POINT"]
        trbf = cfg.get("TARGET_ROOT_BUFFER_FOLDER", "")

        def second_pass(obj):
            if isinstance(obj, dict):
                for kk, vv in obj.items():
                    obj[kk] = second_pass(vv)
                return obj
            elif isinstance(obj, list):
                return [second_pass(i) for i in obj]
            elif isinstance(obj, str):
                return (obj
                        .replace("{MOUNT_POINT}", mp)
                        .replace("{TARGET_ROOT_BUFFER_FOLDER}", trbf))
            else:
                return obj

        second_pass(cfg)

def load_config(path=CONFIG_PATH) -> dict:
    """
    Загружает YAML-файл по пути `path`. Если файл не найден, возвращает пустой dict.
    Затем выполняется fill_placeholders(cfg).
    Возвращает обычный dict без обёртки.
            - #ALL => расшифровать весь файл (получить YAML)
            - #PWD => расшифровать только ENC_PASSWORD
            - иначе считать как обычный YAML (незашифрованный)

    Пример:
        config = load_config()
        print(config["SOURCE_FOLDER"])

    :param path: Путь к YAML-файлу конфигурации. По умолчанию 'settings.yaml'.
    :return: dict со всеми полями из YAML (после плейсхолдеров).
    """
    
    if not os.path.exists(path):
        print(f"[WARN] Файл {path} не найден. Возвращаем пустой словарь.")
        return {}

    # Читаем первую строку (в бинарном режиме), чтобы проверить метку.
    with open(path, "rb") as f:
        header_line = f.readline()

    if header_line.startswith(b"#ALL"):
        # Файл целиком зашифрован
        with open(path, "rb") as ef:
            enc_data = ef.read()
        try:
            dec_data = cipher.decrypt(enc_data)
            if dec_data.startswith(b"#ALL\n"):
                dec_data = dec_data[len(b"#ALL\n"):]
            else:
                print("[WARN] Нет ожидаемой метки #ALL в расшифрованном тексте.")

            raw_cfg = yaml.safe_load(dec_data.decode("utf-8")) or {}
            fill_placeholders(raw_cfg)
            return raw_cfg  # <--- Возвращаем просто dict

        except Exception as e:
            print(f"[ERR] Ошибка расшифровки всего файла {path}: {e}")
            return {}

    elif header_line.startswith(b"#PWD"):
        # Файл шифрует только password => ENC_PASSWORD
        with open(path, "rb") as f2:
            all_data = f2.read()
        lines = all_data.split(b'\n')
        content_nohdr = b'\n'.join(lines[1:])

        try:
            raw_cfg = yaml.safe_load(content_nohdr.decode("utf-8")) or {}
        except Exception as e:
            print(f"[ERR] Ошибка парсинга YAML (#PWD) в {path}: {e}")
            return {}

        # Расшифруем ENC_PASSWORD
        conn = raw_cfg.get("CONNECTION", {})
        enc_val = conn.get("ENC_PASSWORD")
        if enc_val:
            try:
                dec_pass = cipher.decrypt(enc_val.encode()).decode()
                conn["ENC_PASSWORD"] = dec_pass
            except Exception as e:
                print("[ERR] Не удалось расшифровать ENC_PASSWORD:", e)
        raw_cfg["CONNECTION"] = conn

        fill_placeholders(raw_cfg)
        return raw_cfg  # <--- Возвращаем dict

    else:
        # Файл не зашифрован (нет #ALL, нет #PWD)
        with open(path, "r", encoding="utf-8") as f:
            try:
                raw_cfg = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"[ERR] Ошибка парсинга незашифр. YAML {path}: {e}")
                return {}
        fill_placeholders(raw_cfg)
        return raw_cfg  # <--- Возвращаем dict


def encrypt_all_config(path=CONFIG_PATH):
    """
    Шифрует весь файл, добавляя #ALL\n в начало, 
    затем всё шифруем Fernet(KEY).
    """
    backup = path + ".bak"
    shutil.copy2(path, backup)
    time.sleep(1)

    with open(path, "rb") as f:
        original = f.read()

    # prepend #ALL
    data_to_enc = b"#ALL\n" + original
    enc_data = cipher.encrypt(data_to_enc)

    with open(path, "wb") as fw:
        fw.write(enc_data)

    print(f"[INFO] Файл {path} полностью зашифрован. Оригинал -> {backup}")

def encrypt_only_password(path=CONFIG_PATH):
    """
    Считывает YAML, ищет CONNECTION.password, шифрует -> ENC_PASSWORD,
    добавляет #PWD\n как первую строку.
    """
    backup = path + ".bak"
    shutil.copy2(path, backup)
    time.sleep(1)

    with open(path, "rb") as f:
        all_bytes = f.read()

    lines = all_bytes.split(b'\n')
    # Если первая строка не #ALL/#PWD, вставим #PWD
    if not lines or (not lines[0].startswith(b"#ALL") and not lines[0].startswith(b"#PWD")):
        lines.insert(0, b"#PWD")

    # Сконструируем
    nohdr = b'\n'.join(lines[1:])
    try:
        raw_cfg = yaml.safe_load(nohdr.decode("utf-8")) or {}
    except Exception as e:
        print("[ERR] Ошибка парсинга YAML при encrypt_only_password:", e)
        return

    conn = raw_cfg.get("CONNECTION", {})
    pwd = conn.get("password")
    if pwd:
        enc_val = cipher.encrypt(pwd.encode()).decode()
        conn["ENC_PASSWORD"] = enc_val
        del conn["password"]
        raw_cfg["CONNECTION"] = conn
        print("[INFO] Поле password -> ENC_PASSWORD успешно зашифровано.")
    else:
        print("[WARN] Нет поля CONNECTION.password, нечего шифровать.")

    # Записываем итог
    final_text = "#PWD\n" + yaml.safe_dump(raw_cfg, sort_keys=False, allow_unicode=True)
    with open(path, "w", encoding="utf-8") as fw:
        fw.write(final_text)

    print(f"[INFO] Обновлён {path}, оригинал {backup}")


def save_config(config: dict, path=CONFIG_PATH):
    """
    Сохраняем конфигурацию (dict) обратно в YAML.

    ВАЖНО: если в конфиге изначально были плейсхолдеры (например, '{EXTNAME}'),
    после сохранения они будут перезаписаны конечными значениями,
    т.к. fill_placeholders уже подменил их.

    Пример:
        cfg = load_config()
        cfg["SOURCE_FOLDER"] = "/new/path"
        save_config(cfg)

    :param config: dict с параметрами конфигурации
    :param path: Куда сохранить YAML (по умолчанию CONFIG_PATH)
    """
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)

    print(f"Конфигурация сохранена в '{path}'.")
