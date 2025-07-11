#!/bin/bash
# Обновленный скрипт установки супервизора миграции с исправлениями блокировок
# Поддерживает новую архитектуру файлов состояния и диагностические инструменты

set -e

# Константы - настройте под ваше окружение
MAIN_SCRIPT_PATH="/opt/astra_migration_tool/main.py"
VENV_PATH="/opt/astra_migration_tool/venv"
STATE_FILE_PATH="/var/lib/migration-service/state.json"
SERVICE_USER="root"
SERVICE_GROUP="root"
INSTALL_DIR="/opt/migration"
LOG_DIR="/var/log/migration-supervisor"
DATA_DIR="/var/lib/migration-service"
SERVICE_NAME="migration-supervisor"
AUTO_ENABLE_SERVICE="true"

# НОВОЕ: Дополнительные пути для исправленной архитектуры
NETWORK_STATE_FILE="/mnt/migration/migration_state.json"
LOCAL_STATE_FILE="/tmp/migration_state.json"
SERVICE_MINIMAL_FILE="/var/lib/migration-service/current_state.json"
SUPERVISOR_READ_FILE="/var/lib/migration-service/supervisor_state.json"

echo "=== Установка супервизора миграции ==="
echo "Основной скрипт: $MAIN_SCRIPT_PATH"
echo "Виртуальное окружение: $VENV_PATH"
echo "Файлы состояния:"
echo "  - Основной: $STATE_FILE_PATH"
echo "  - Минимальный: $SERVICE_MINIMAL_FILE"
echo "  - Супервизор: $SUPERVISOR_READ_FILE"
echo "  - Локальный: $LOCAL_STATE_FILE"
echo

# Проверка прав root
if [[ $EUID -ne 0 ]]; then
    echo "Ошибка: Запустите от root"
    exit 1
fi

# Функция резервного копирования
backup_existing_files() {
    echo "Создание резервных копий существующих файлов..."
    local backup_dir="/opt/migration/backup_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$backup_dir"
    
    # Список файлов для резервного копирования
    local files_to_backup=(
        "$INSTALL_DIR/migration_supervisor.py"
        "$MAIN_SCRIPT_PATH"
        "/opt/astra_migration_tool/src/migration/state_tracker.py"
    )
    
    for file in "${files_to_backup[@]}"; do
        if [[ -f "$file" ]]; then
            cp "$file" "$backup_dir/" 2>/dev/null || true
            echo "✓ Резервная копия: $(basename "$file")"
        fi
    done
    
    echo "Резервные копии сохранены в: $backup_dir"
}

# Установка системных зависимостей
install_dependencies() {
    echo "1. Установка системных зависимостей..."
    apt-get update -qq
    apt-get install -y python3 python3-venv python3-pip lsof psmisc
    echo "✓ Зависимости установлены"
}

# Создание директорий
create_directories() {
    echo "2. Создание директорий..."
    
    local directories=(
        "$INSTALL_DIR"
        "$LOG_DIR"
        "$DATA_DIR"
        "$DATA_DIR/backups"
        "$(dirname "$STATE_FILE_PATH")"
        "$(dirname "$SERVICE_MINIMAL_FILE")"
        "$(dirname "$SUPERVISOR_READ_FILE")"
        "$(dirname "$VENV_PATH")"
        "$(dirname "$MAIN_SCRIPT_PATH")"
        "$(dirname "$LOCAL_STATE_FILE")"
        "/var/run"
    )
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        echo "✓ Создана директория: $dir"
    done
    
    # Установка прав
    chown "$SERVICE_USER:$SERVICE_GROUP" "$LOG_DIR" "$DATA_DIR"
    chmod 755 "$DATA_DIR"
    chmod 755 "$LOG_DIR"
    
    echo "✓ Права доступа настроены"
}

# Установка исправленного супервизора
install_supervisor() {
    echo "3. Установка исправленного супервизора..."
    
    # Поиск файла супервизора в порядке приоритета
    local supervisor_files=(
        "migration_supervisor_fixed.py"
        "fixed_supervisor.py"
        "improved_supervisor.py"
        "migration_supervisor.py"
    )
    
    local supervisor_source=""
    for file in "${supervisor_files[@]}"; do
        if [[ -f "$file" ]]; then
            supervisor_source="$file"
            break
        fi
    done
    
    if [[ -z "$supervisor_source" ]]; then
        echo "Ошибка: Файл супервизора не найден"
        echo "Ожидаемые файлы: ${supervisor_files[*]}"
        exit 1
    fi
    
    echo "Используем супервизор: $supervisor_source"
    
    # Создаем супервизор с правильными путями
    sed \
        -e "s|MAIN_SCRIPT = \".*\"|MAIN_SCRIPT = \"$MAIN_SCRIPT_PATH\"|" \
        -e "s|VENV_PATH = \".*\"|VENV_PATH = \"$VENV_PATH\"|" \
        -e "s|STATE_FILE = \".*\"|STATE_FILE = \"$STATE_FILE_PATH\"|" \
        -e "s|SERVICE_STATE_FILE = \".*\"|SERVICE_STATE_FILE = \"$STATE_FILE_PATH\"|" \
        -e "s|SUPERVISOR_READ_FILE = \".*\"|SUPERVISOR_READ_FILE = \"$SUPERVISOR_READ_FILE\"|" \
        -e "s|LOG_FILE = \".*\"|LOG_FILE = \"$LOG_DIR/migration-supervisor.log\"|" \
        -e "s|PID_FILE = \".*\"|PID_FILE = \"/var/run/migration-supervisor.pid\"|" \
        "$supervisor_source" > "$INSTALL_DIR/migration_supervisor.py"
    
    chmod 755 "$INSTALL_DIR/migration_supervisor.py"
    chown "$SERVICE_USER:$SERVICE_GROUP" "$INSTALL_DIR/migration_supervisor.py"
    
    echo "✓ Супервизор установлен"
}

# Установка диагностических инструментов
install_diagnostic_tools() {
    echo "4. Установка диагностических инструментов..."
    
    # Поиск диагностического скрипта
    local diagnostic_files=(
        "file_lock_diagnostic.py"
        "migration_diagnostic.py"
        "diagnostic.py"
    )
    
    local diagnostic_source=""
    for file in "${diagnostic_files[@]}"; do
        if [[ -f "$file" ]]; then
            diagnostic_source="$file"
            break
        fi
    done
    
    if [[ -n "$diagnostic_source" ]]; then
        # Обновляем пути в диагностическом скрипте
        sed \
            -e "s|'network': '.*'|'network': '$NETWORK_STATE_FILE'|" \
            -e "s|'local_tmp': '.*'|'local_tmp': '$LOCAL_STATE_FILE'|" \
            -e "s|'service_full': '.*'|'service_full': '$STATE_FILE_PATH'|" \
            -e "s|'service_minimal': '.*'|'service_minimal': '$SERVICE_MINIMAL_FILE'|" \
            -e "s|'supervisor': '.*'|'supervisor': '$SUPERVISOR_READ_FILE'|" \
            "$diagnostic_source" > "$INSTALL_DIR/file_lock_diagnostic.py"
        
        chmod 755 "$INSTALL_DIR/file_lock_diagnostic.py"
        chown "$SERVICE_USER:$SERVICE_GROUP" "$INSTALL_DIR/file_lock_diagnostic.py"
        
        echo "✓ Диагностический инструмент установлен: $diagnostic_source"
    else
        echo "⚠ Диагностический скрипт не найден, создаем базовую версию..."
        
        # Создаем минимальный диагностический скрипт
        cat > "$INSTALL_DIR/file_lock_diagnostic.py" << 'EOF'
#!/usr/bin/env python3
"""Базовый диагностический скрипт для проблем с блокировками"""
import os, sys, json

STATE_FILES = {
    'service_full': '/var/lib/migration-service/state.json',
    'service_minimal': '/var/lib/migration-service/current_state.json',
    'supervisor': '/var/lib/migration-service/supervisor_state.json',
    'local_tmp': '/tmp/migration_state.json'
}

def check_files():
    print("=== Проверка файлов состояния ===")
    for name, path in STATE_FILES.items():
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    json.load(f)
                print(f"✓ {name}: {path}")
            except Exception as e:
                print(f"✗ {name}: {path} - Ошибка: {e}")
        else:
            print(f"⚠ {name}: {path} - Не найден")

def fix_locks():
    print("=== Очистка блокировок ===")
    import glob
    for lock_file in glob.glob('/var/lib/migration-service/*.lock'):
        try:
            os.remove(lock_file)
            print(f"✓ Удален: {lock_file}")
        except Exception as e:
            print(f"✗ Ошибка удаления {lock_file}: {e}")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--fix-locks':
        fix_locks()
    else:
        check_files()
EOF
        
        chmod 755 "$INSTALL_DIR/file_lock_diagnostic.py"
        echo "✓ Базовый диагностический скрипт создан"
    fi
}

# Создание systemd service
create_systemd_service() {
    echo "5. Создание systemd service..."
    
    cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=Migration Data Supervisor (Fixed Locks Version)
After=network.target multi-user.target
Wants=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 $INSTALL_DIR/migration_supervisor.py
ExecStop=/usr/bin/python3 $INSTALL_DIR/migration_supervisor.py stop
ExecReload=/bin/kill -HUP \$MAINPID
Restart=no
RestartSec=10

User=$SERVICE_USER
Group=$SERVICE_GROUP

# Настройки для предотвращения блокировок
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Переменные окружения
Environment=PYTHONPATH=/opt/astra_migration_tool
Environment=MIGRATION_LOG_LEVEL=INFO

[Install]
WantedBy=multi-user.target
EOF
    
    chmod 644 "/etc/systemd/system/$SERVICE_NAME.service"
    systemctl daemon-reload
    
    echo "✓ Systemd service создан с поддержкой исправлений"
}

# Создание улучшенных утилит управления
create_management_tools() {
    echo "6. Создание улучшенных утилит управления..."
    
    # Основная утилита управления
    cat > /usr/local/bin/migration-ctl << EOF
#!/bin/bash
SERVICE_NAME="migration-supervisor"
SUPERVISOR_SCRIPT="$INSTALL_DIR/migration_supervisor.py"
DIAGNOSTIC_SCRIPT="$INSTALL_DIR/file_lock_diagnostic.py"

case "\$1" in
    start)
        echo "Запуск супервизора..."
        # Предварительная очистка блокировок
        python3 "\$DIAGNOSTIC_SCRIPT" --fix-locks 2>/dev/null || true
        systemctl start \$SERVICE_NAME
        ;;
    stop)
        echo "Остановка супервизора..."
        systemctl stop \$SERVICE_NAME
        # Очистка после остановки
        sleep 2
        python3 "\$DIAGNOSTIC_SCRIPT" --fix-locks 2>/dev/null || true
        ;;
    restart)
        echo "Перезапуск супервизора..."
        systemctl stop \$SERVICE_NAME
        sleep 2
        python3 "\$DIAGNOSTIC_SCRIPT" --fix-locks 2>/dev/null || true
        systemctl start \$SERVICE_NAME
        ;;
    status)
        echo "=== Статус службы ==="
        systemctl status \$SERVICE_NAME --no-pager -l
        echo
        echo "=== Статус супервизора ==="
        python3 "\$SUPERVISOR_SCRIPT" status 2>/dev/null || echo "Супервизор не отвечает"
        echo
        echo "=== Проверка файлов ==="
        python3 "\$DIAGNOSTIC_SCRIPT" --check-locks 2>/dev/null || echo "Диагностика недоступна"
        ;;
    logs)
        echo "Логи супервизора..."
        journalctl -u \$SERVICE_NAME -f --no-pager
        ;;
    logs-migration)
        echo "Логи миграции..."
        tail -f $LOG_DIR/migration.log 2>/dev/null || echo "Лог миграции не найден"
        ;;
    enable)
        systemctl enable \$SERVICE_NAME
        echo "Автозапуск включен"
        ;;
    disable)
        systemctl disable \$SERVICE_NAME
        echo "Автозапуск отключен"
        ;;
    check)
        echo "Быстрая проверка..."
        if systemctl is-active \$SERVICE_NAME >/dev/null 2>&1; then
            echo "✓ Служба активна"
        else
            echo "✗ Служба неактивна"
        fi
        python3 "\$DIAGNOSTIC_SCRIPT" 2>/dev/null || echo "Диагностика недоступна"
        ;;
    fix)
        echo "Исправление проблем с блокировками..."
        systemctl stop \$SERVICE_NAME 2>/dev/null || true
        python3 "\$DIAGNOSTIC_SCRIPT" --fix-locks
        echo "Готово. Запустите: migration-ctl start"
        ;;
    monitor)
        echo "Мониторинг файлов состояния (60 сек)..."
        python3 "\$DIAGNOSTIC_SCRIPT" --monitor 60 2>/dev/null || echo "Мониторинг недоступен"
        ;;
    *)
        echo "Утилита управления супервизором миграции (версия с исправлениями блокировок)"
        echo ""
        echo "Команды:"
        echo "  start, stop, restart  - Управление службой"
        echo "  status, logs, check   - Мониторинг"
        echo "  enable, disable       - Автозапуск"
        echo "  fix                   - Исправление блокировок"
        echo "  monitor               - Мониторинг изменений файлов"
        echo "  logs-migration        - Логи основного скрипта"
        exit 1
        ;;
esac
EOF
    
    chmod 755 /usr/local/bin/migration-ctl
    
    # Расширенная утилита проверки
    cat > /usr/local/bin/migration-check << EOF
#!/bin/bash
echo "=== Проверка состояния миграции (версия с исправлениями) ==="

# Проверка службы
if systemctl is-active migration-supervisor >/dev/null 2>&1; then
    echo "✓ Сервис активен"
else
    echo "✗ Сервис неактивен"
fi

# Проверка основного скрипта
if [[ -f "$MAIN_SCRIPT_PATH" ]]; then
    echo "✓ Основной скрипт найден"
else
    echo "✗ Основной скрипт не найден: $MAIN_SCRIPT_PATH"
fi

# Проверка виртуального окружения
if [[ -f "$VENV_PATH/bin/python" ]]; then
    echo "✓ Виртуальное окружение найдено"
else
    echo "⚠ Виртуальное окружение не найдено: $VENV_PATH"
fi

# Проверка файлов состояния
echo
echo "=== Файлы состояния ==="
state_files=(
    "Основной:$STATE_FILE_PATH"
    "Минимальный:$SERVICE_MINIMAL_FILE"
    "Супервизор:$SUPERVISOR_READ_FILE"
    "Локальный:$LOCAL_STATE_FILE"
)

for entry in "\${state_files[@]}"; do
    name="\${entry%%:*}"
    file="\${entry#*:}"
    if [[ -f "\$file" ]]; then
        size=\$(stat -c%s "\$file" 2>/dev/null || echo "0")
        echo "✓ \$name: \$file (\${size} байт)"
    else
        echo "⚠ \$name: \$file (отсутствует)"
    fi
done

# Проверка блокировок
echo
echo "=== Проверка блокировок ==="
lock_count=\$(find $DATA_DIR -name "*.lock" 2>/dev/null | wc -l)
if [[ \$lock_count -eq 0 ]]; then
    echo "✓ Активных блокировок не найдено"
else
    echo "⚠ Найдено блокировок: \$lock_count"
    find $DATA_DIR -name "*.lock" 2>/dev/null | head -5
fi

# Проверка процессов
echo
echo "=== Процессы миграции ==="
migration_pids=\$(pgrep -f migration 2>/dev/null || true)
if [[ -n "\$migration_pids" ]]; then
    echo "Активные процессы: \$migration_pids"
    for pid in \$migration_pids; do
        cmd=\$(ps -p \$pid -o comm= 2>/dev/null || echo "неизвестно")
        echo "  PID \$pid: \$cmd"
    done
else
    echo "Процессы миграции не найдены"
fi

echo
echo "Для управления: migration-ctl"
echo "Для исправления проблем: migration-ctl fix"
EOF
    
    chmod 755 /usr/local/bin/migration-check
    
    echo "✓ Улучшенные утилиты управления созданы"
}

# Настройка логирования
setup_logging() {
    echo "7. Настройка логирования..."
    
    cat > /etc/logrotate.d/migration-supervisor << EOF
$LOG_DIR/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 640 $SERVICE_USER $SERVICE_GROUP
    sharedscripts
    postrotate
        systemctl reload-or-restart migration-supervisor 2>/dev/null || true
    endscript
}
EOF
    
    # Настройка rsyslog для отдельного лога
    if [[ -d /etc/rsyslog.d ]]; then
        cat > /etc/rsyslog.d/49-migration.conf << EOF
# Логи миграции
if \$programname == 'migration-supervisor' then $LOG_DIR/migration-supervisor.log
& stop
EOF
        systemctl reload rsyslog 2>/dev/null || true
    fi
    
    echo "✓ Логирование настроено"
}

# Создание скрипта для обновления исправлений
create_update_script() {
    echo "8. Создание скрипта обновления..."
    
    cat > "$INSTALL_DIR/update_fixes.sh" << 'EOF'
#!/bin/bash
# Скрипт для обновления исправлений блокировок

INSTALL_DIR="/opt/migration"
MAIN_PROJECT_DIR="/opt/astra_migration_tool"

echo "=== Обновление исправлений блокировок ==="

# Остановка сервисов
echo "Остановка сервисов..."
systemctl stop migration-supervisor 2>/dev/null || true

# Обновление state_tracker.py если есть исправленная версия
if [[ -f "state_tracker_fixed.py" ]]; then
    echo "Обновление state_tracker.py..."
    cp "state_tracker_fixed.py" "$MAIN_PROJECT_DIR/src/migration/state_tracker.py"
    echo "✓ state_tracker.py обновлен"
fi

# Обновление супервизора
if [[ -f "migration_supervisor_fixed.py" ]]; then
    echo "Обновление супервизора..."
    cp "migration_supervisor_fixed.py" "$INSTALL_DIR/migration_supervisor.py"
    chmod 755 "$INSTALL_DIR/migration_supervisor.py"
    echo "✓ Супервизор обновлен"
fi

# Обновление диагностического скрипта
if [[ -f "file_lock_diagnostic.py" ]]; then
    echo "Обновление диагностического скрипта..."
    cp "file_lock_diagnostic.py" "$INSTALL_DIR/"
    chmod 755 "$INSTALL_DIR/file_lock_diagnostic.py"
    echo "✓ Диагностический скрипт обновлен"
fi

# Очистка старых блокировок
echo "Очистка блокировок..."
python3 "$INSTALL_DIR/file_lock_diagnostic.py" --fix-locks 2>/dev/null || true

# Перезапуск
echo "Перезапуск сервисов..."
systemctl daemon-reload
systemctl start migration-supervisor

echo "✓ Обновление завершено"
EOF
    
    chmod 755 "$INSTALL_DIR/update_fixes.sh"
    echo "✓ Скрипт обновления создан: $INSTALL_DIR/update_fixes.sh"
}

# Финальная проверка
final_verification() {
    echo "9. Проверка установки..."
    
    errors=0
    
    # Проверка файлов
    files_to_check=(
        "$INSTALL_DIR/migration_supervisor.py"
        "$INSTALL_DIR/file_lock_diagnostic.py"
        "/etc/systemd/system/$SERVICE_NAME.service"
        "/usr/local/bin/migration-ctl"
        "/usr/local/bin/migration-check"
    )
    
    for file in "${files_to_check[@]}"; do
        if [[ -f "$file" ]]; then
            echo "✓ $file"
        else
            echo "✗ $file отсутствует"
            ((errors++))
        fi
    done
    
    # Проверка директорий
    dirs_to_check=(
        "$INSTALL_DIR"
        "$LOG_DIR"
        "$DATA_DIR"
    )
    
    for dir in "${dirs_to_check[@]}"; do
        if [[ -d "$dir" ]]; then
            echo "✓ $dir"
        else
            echo "✗ $dir отсутствует"
            ((errors++))
        fi
    done
    
    # Проверка systemd service
    if systemctl list-unit-files | grep -q "$SERVICE_NAME"; then
        echo "✓ Systemd service зарегистрирован"
    else
        echo "✗ Systemd service не зарегистрирован"
        ((errors++))
    fi
    
    # Проверка диагностического скрипта
    if python3 "$INSTALL_DIR/file_lock_diagnostic.py" --help >/dev/null 2>&1; then
        echo "✓ Диагностический скрипт работает"
    else
        echo "⚠ Диагностический скрипт может работать некорректно"
    fi
    
    return $errors
}

# Главная функция
main() {
    echo "Начало установки..."
    
    # Создаем резервные копии
    backup_existing_files
    
    # Основные шаги установки
    install_dependencies
    create_directories
    install_supervisor
    install_diagnostic_tools
    create_systemd_service
    create_management_tools
    setup_logging
    create_update_script
    
    # Проверка
    if final_verification; then
        local errors=$?
        if [[ $errors -eq 0 ]]; then
            echo
            echo "✅ Установка с исправлениями блокировок завершена успешно!"
            echo
            echo
            echo "📁 Конфигурация:"
            echo "  Основной скрипт:       $MAIN_SCRIPT_PATH"
            echo "  Виртуальное окружение: $VENV_PATH"
            echo "  Файлы состояния:"
            echo "    - Основной:          $STATE_FILE_PATH"
            echo "    - Минимальный:       $SERVICE_MINIMAL_FILE"
            echo "    - Супервизор:        $SUPERVISOR_READ_FILE"
            echo "    - Локальный:         $LOCAL_STATE_FILE"
            echo "  Логи:                  $LOG_DIR/"
            echo
            echo "🎯 Управление:"
            echo "  migration-ctl start   # Запуск"
            echo "  migration-ctl status  # Подробный статус"
            echo "  migration-ctl fix     # Исправление блокировок"
            echo "  migration-ctl monitor # Мониторинг файлов"
            echo "  migration-check       # Быстрая проверка"
            echo
            echo "🔄 Следующие шаги:"
            echo "  1. Убедитесь что основной скрипт находится в: $MAIN_SCRIPT_PATH"
            echo "  2. Обновите state_tracker.py исправленной версией"
            echo "  3. Создайте виртуальное окружение: python3 -m venv $VENV_PATH"
            echo "  4. Проверьте: migration-check"
            echo "  5. Запустите: migration-ctl start"
            echo
            echo "🛠️ Диагностика:"
            echo "  При проблемах: migration-ctl fix"
            echo "  Полная диагностика: python3 $INSTALL_DIR/file_lock_diagnostic.py --all"
            
            # Включение автозапуска
            if [[ "$AUTO_ENABLE_SERVICE" == "true" ]]; then
                echo
                echo "🚀 Включение автозапуска..."
                systemctl enable "$SERVICE_NAME" >/dev/null 2>&1
                echo "✓ Автозапуск включен"
            fi
            
            exit 0
        else
            echo
            echo "⚠️ Установка завершилась с $errors предупреждениями"
            echo "Система может работать, но рекомендуется проверить проблемы"
            exit 0
        fi
    else
        echo
        echo "❌ Установка завершилась с критическими ошибками"
        exit 1
    fi
}

# Запуск установки
main "$@"