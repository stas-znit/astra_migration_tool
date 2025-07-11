from src.config.config_loader import load_config
import json
import os

from src.migration.state_tracker import load_state

def check_state_files():
        
        print("=" * 50)
        print("ПРОВЕРКА ФАЙЛОВ СОСТОЯНИЯ")
        print("=" * 50)
        
        config = load_config()
        network_state_file = config["STATE_FILE"]
        
        files_to_check = [
            ("Сетевой файл состояния", network_state_file),
            ("Локальный файл /tmp", "/tmp/migration_state.json"),
            ("Файл сервиса", "/var/lib/migration-service/state.json"),
            ("Минимальный файл сервиса", "/var/lib/migration-service/current_state.json")
        ]
        
        for name, filepath in files_to_check:
            print(f"\n{name}: {filepath}")
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    print(f"  ✓ Файл существует и валиден")
                    print(f"  ✓ Пользователей в состоянии: {len(data.get('users', {}))}")
                    print(f"  ✓ Глобальный статус: {data.get('global', {}).get('status', 'не указан')}")
                    
                    # Показываем пользователей
                    users_state = data.get('users', {})
                    if users_state:
                        print(f"  ✓ Статусы пользователей:")
                        for user, status in users_state.items():
                            print(f"      {user}: {status}")
                    else:
                        print(f"  ⚠ Нет данных о пользователях")
                        
                except Exception as e:
                    print(f"  ✗ Ошибка чтения: {e}")
            else:
                print(f"  ✗ Файл не существует")
        
        print("=" * 50)

def debug_migration_state():
        
        print("=" * 60)
        print("ОТЛАДКА: Проверка состояния перед началом миграции")
        print("=" * 60)
        
        # Загружаем состояние
        state = load_state()
        config = load_config()
        network_state_file = config["STATE_FILE"]
        
        print(f"Состояние загружено из файла: {network_state_file}")
        print(f"Глобальное состояние: {json.dumps(state.get('global', {}), ensure_ascii=False, indent=2)}")
        print(f"Состояние пользователей: {json.dumps(state.get('users', {}), ensure_ascii=False, indent=2)}")
        
        return state