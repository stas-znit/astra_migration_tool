#!/bin/bash
# Пример настройки константов для массового развертывания
# Скопируйте этот файл как config.sh и настройте под ваше окружение

# =============================================================================
# НАСТРОЙКИ ПУТЕЙ - ИЗМЕНИТЕ ПОД ВАШЕ ОКРУЖЕНИЕ
# =============================================================================

# Основной скрипт миграции
export MAIN_SCRIPT_PATH="/opt/astra_migration_tool/main.py"

# Виртуальное окружение Python
export VENV_PATH="/opt/astra_migration_tool/venv"

# Файл состояния миграции (где основной скрипт пишет heartbeat)
export STATE_FILE_PATH="/var/lib/migration-service/state.json"

# =============================================================================
# НАСТРОЙКИ СЕРВИСА
# =============================================================================

# Пользователь для запуска (обычно root для системных операций)
export SERVICE_USER="root"
export SERVICE_GROUP="root"

# Название сервиса в systemd
export SERVICE_NAME="migration-supervisor"

# Автоматически включать автозапуск при установке
export AUTO_ENABLE_SERVICE="true"  # true/false

# =============================================================================
# НАСТРОЙКИ ДИРЕКТОРИЙ
# =============================================================================

# Где устанавливать супервизор
export INSTALL_DIR="/opt/migration"

# Где хранить логи
export LOG_DIR="/var/log/migration-supervisor"

# Где хранить данные супервизора
export DATA_DIR="/var/lib/migration-supervisor"

# =============================================================================
# НАСТРОЙКИ МОНИТОРИНГА
# =============================================================================

# Таймаут heartbeat (секунды) - как долго ждать сигнала от основного скрипта
export HEARTBEAT_TIMEOUT="120"

# Интервал проверок (секунды) - как часто проверять состояние
export CHECK_INTERVAL="30"

# Максимальное количество автоматических перезапусков
export MAX_RESTARTS="3"

# Задержка между перезапусками (секунды)
export RESTART_DELAY="60"

# =============================================================================
# ПРИМЕРЫ КОНФИГУРАЦИЙ ДЛЯ РАЗНЫХ СЦЕНАРИЕВ
# =============================================================================

# Сценарий 1: Стандартная установка
setup_standard() {
    export MAIN_SCRIPT_PATH="/opt/astra_migration_tool/main.py"
    export VENV_PATH="/opt/astra_migration_tool/venv"
    export STATE_FILE_PATH="/var/lib/migration-service/state.json"
    export AUTO_ENABLE_SERVICE="true"
}

# Сценарий 2: Установка в пользовательскую директорию
setup_custom_dir() {
    export MAIN_SCRIPT_PATH="/home/migration/app/main.py"
    export VENV_PATH="/home/migration/app/venv"
    export STATE_FILE_PATH="/home/migration/data/state.json"
    export SERVICE_USER="migration"
    export SERVICE_GROUP="migration"
    export INSTALL_DIR="/home/migration/supervisor"
    export LOG_DIR="/home/migration/logs"
    export DATA_DIR="/home/migration/data"
}

# Сценарий 3: Режим разработки/тестирования
setup_development() {
    export MAIN_SCRIPT_PATH="/opt/migration-dev/main.py"
    export VENV_PATH="/opt/migration-dev/venv"
    export STATE_FILE_PATH="/tmp/migration_state.json"
    export SERVICE_NAME="migration-supervisor-dev"
    export AUTO_ENABLE_SERVICE="false"
    export HEARTBEAT_TIMEOUT="60"  # Более частые проверки
    export CHECK_INTERVAL="15"
    export MAX_RESTARTS="1"        # Меньше перезапусков для отладки
}

# Сценарий 4: Высоконагруженная система
setup_high_load() {
    export HEARTBEAT_TIMEOUT="180"  # Больше времени на операции
    export CHECK_INTERVAL="45"
    export MAX_RESTARTS="5"         # Больше попыток восстановления
    export RESTART_DELAY="120"      # Больше времени между перезапусками
}

# =============================================================================
# ФУНКЦИИ ПРИМЕНЕНИЯ НАСТРОЕК
# =============================================================================

# Применить стандартные настройки
apply_standard() {
    setup_standard
    echo "Применены стандартные настройки"
}

# Применить пользовательские настройки
apply_custom() {
    setup_custom_dir
    echo "Применены пользовательские настройки"
}

# Применить настройки для разработки
apply_development() {
    setup_development
    echo "Применены настройки для разработки"
}

# Показать текущие настройки
show_config() {
    echo "=== Текущие настройки ==="
    echo "Основной скрипт:      $MAIN_SCRIPT_PATH"
    echo "Виртуальное окружение: $VENV_PATH"
    echo "Файл состояния:       $STATE_FILE_PATH"
    echo "Пользователь сервиса: $SERVICE_USER"
    echo "Название сервиса:     $SERVICE_NAME"
    echo "Автозапуск:           $AUTO_ENABLE_SERVICE"
    echo "Директория установки: $INSTALL_DIR"
    echo "Директория логов:     $LOG_DIR"
    echo "Таймаут heartbeat:    $HEARTBEAT_TIMEOUT сек"
    echo "Интервал проверок:    $CHECK_INTERVAL сек"
    echo "Макс. перезапусков:   $MAX_RESTARTS"
}

# =============================================================================
# ВАЛИДАЦИЯ НАСТРОЕК
# =============================================================================

validate_config() {
    local errors=0
    
    echo "Проверка настроек..."
    
    # Проверяем обязательные переменные
    local required_vars=(
        "MAIN_SCRIPT_PATH"
        "VENV_PATH"
        "STATE_FILE_PATH"
        "SERVICE_USER"
        "INSTALL_DIR"
        "LOG_DIR"
    )
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var}" ]]; then
            echo "✗ Не задана переменная: $var"
            ((errors++))
        fi
    done
    
    # Проверяем числовые значения
    local numeric_vars=(
        "HEARTBEAT_TIMEOUT"
        "CHECK_INTERVAL"
        "MAX_RESTARTS"
        "RESTART_DELAY"
    )
    
    for var in "${numeric_vars[@]}"; do
        if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
            echo "✗ Переменная $var должна быть числом: ${!var}"
            ((errors++))
        fi
    done
    
    # Проверяем логические значения
    if [[ "$AUTO_ENABLE_SERVICE" != "true" && "$AUTO_ENABLE_SERVICE" != "false" ]]; then
        echo "✗ AUTO_ENABLE_SERVICE должно быть 'true' или 'false': $AUTO_ENABLE_SERVICE"
        ((errors++))
    fi
    
    if [[ $errors -eq 0 ]]; then
        echo "✓ Настройки корректны"
        return 0
    else
        echo "Обнаружено $errors ошибок в настройках"
        return 1
    fi
}

# =============================================================================
# ПРИМЕНЕНИЕ НАСТРОЕК К СКРИПТАМ
# =============================================================================

update_setup_script() {
    local setup_file="setup.sh"
    
    if [[ ! -f "$setup_file" ]]; then
        echo "Файл $setup_file не найден"
        return 1
    fi
    
    echo "Обновление $setup_file с текущими настройками..."
    
    # Создаем backup
    cp "$setup_file" "$setup_file.backup"
    
    # Обновляем константы в скрипте установки
    sed -i "s|^MAIN_SCRIPT_PATH=.*|MAIN_SCRIPT_PATH=\"$MAIN_SCRIPT_PATH\"|" "$setup_file"
    sed -i "s|^VENV_PATH=.*|VENV_PATH=\"$VENV_PATH\"|" "$setup_file"
    sed -i "s|^STATE_FILE_PATH=.*|STATE_FILE_PATH=\"$STATE_FILE_PATH\"|" "$setup_file"
    sed -i "s|^SERVICE_USER=.*|SERVICE_USER=\"$SERVICE_USER\"|" "$setup_file"
    sed -i "s|^SERVICE_GROUP=.*|SERVICE_GROUP=\"$SERVICE_GROUP\"|" "$setup_file"
    sed -i "s|^AUTO_ENABLE_SERVICE=.*|AUTO_ENABLE_SERVICE=\"$AUTO_ENABLE_SERVICE\"|" "$setup_file"
    sed -i "s|^INSTALL_DIR=.*|INSTALL_DIR=\"$INSTALL_DIR\"|" "$setup_file"
    sed -i "s|^LOG_DIR=.*|LOG_DIR=\"$LOG_DIR\"|" "$setup_file"
    sed -i "s|^DATA_DIR=.*|DATA_DIR=\"$DATA_DIR\"|" "$setup_file"
    
    echo "✓ Скрипт установки обновлен"
    echo "  Backup: $setup_file.backup"
}

# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

main() {
    case "${1:-show}" in
        "standard")
            apply_standard
            show_config
            ;;
        "custom")
            apply_custom
            show_config
            ;;
        "development")
            apply_development
            show_config
            ;;
        "show")
            show_config
            ;;
        "validate")
            validate_config
            ;;
        "update")
            if validate_config; then
                update_setup_script
            else
                echo "Исправьте ошибки в настройках перед обновлением"
                exit 1
            fi
            ;;
        *)
            echo "Утилита настройки константов для массового развертывания"
            echo ""
            echo "Использование: $0 {standard|custom|development|show|validate|update}"
            echo ""
            echo "Команды:"
            echo "  standard     - Применить стандартные настройки"
            echo "  custom       - Применить пользовательские настройки"
            echo "  development  - Применить настройки для разработки"
            echo "  show         - Показать текущие настройки"
            echo "  validate     - Проверить корректность настроек"
            echo "  update       - Обновить скрипт установки текущими настройками"
            echo ""
            echo "Для ручной настройки отредактируйте переменные в начале файла"
            exit 1
            ;;
    esac
}

# Применяем стандартные настройки по умолчанию
setup_standard

# Запускаем если скрипт вызван напрямую
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi