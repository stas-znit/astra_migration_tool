import sqlite3
import os
import pytest
from src.migration.integrity_checker import load_hashes_from_db

def test_load_hashes_from_db_remove_network_prefix(tmp_path):
    """
    Тестирует, что при наличии в БД пути вида 
    //192.168.81.54/share/EXTNAME/<имя_пользователя>/Документы/file1.txt
    мы корректно убираем префикс //192.168.81.54/share/EXTNAME
    и приклеиваем /home/temp, получая /home/temp/<имя_пользователя>/Документы/file1.txt
    """

    # 1. Создаём временную БД
    db_file = tmp_path / "test_file_hashes.db"
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE file_hashes (
                path TEXT,
                current_hash TEXT
            )
        """)

        # 2. Вставляем примерный путь:
        #    //192.168.81.54/share/EXTNAME/vasya/Документы/file1.txt
        #    c хешом "abc123"
        net_prefix = "//192.168.81.54/share/EXTNAME"
        full_win_path = "//192.168.81.54/share/EXTNAME/vasya/Документы/file1.txt"
        cursor.execute(
            "INSERT INTO file_hashes (path, current_hash) VALUES (?, ?)",
            (full_win_path, "abc123")
        )
        conn.commit()

    # 3. base_path = "/home/temp", network_path = net_prefix
    base_path = "/home/temp"
    network_path = net_prefix

    # 4. Вызываем load_hashes_from_db
    hashes_dict = load_hashes_from_db(
        db_path=str(db_file),
        base_path=base_path,
        network_path=network_path
    )

    # 5. Проверяем результат
    # Исходный путь: //192.168.81.54/share/EXTNAME/vasya/Документы/file1.txt
    # После remove_network_path префикс удалится => /vasya/Документы/file1.txt
    # Затем объединение с base_path="/home/temp" => /home/temp/vasya/Документы/file1.txt
    expected_key = os.path.normpath("/home/temp/vasya/Документы/file1.txt")

    assert len(hashes_dict) == 1, "Ожидаем одну запись."
    assert expected_key in hashes_dict, (
        f"Ожидаем ключ {expected_key} в словаре, имеем: {list(hashes_dict.keys())}"
    )
    assert hashes_dict[expected_key] == "abc123"
