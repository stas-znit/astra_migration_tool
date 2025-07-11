#!/bin/bash
# Скрипт для упаковки основного скрипта миграции в бинарный файл для Linux

# Проверка запуска от имени администратора
if [ "$EUID" -ne 0 ]; then
    echo "ОШИБКА: Этот скрипт должен быть запущен от имени администратора"
    exit 1
fi

# Проверка, что скрипт запущен на Linux
if [[ "$(uname)" != "Linux" ]]; then
    echo "Этот скрипт должен выполняться на Linux для создания Linux-совместимого бинарного файла."
    exit 1
fi

# Вывод информации о системе сборки
echo "Сборка для Linux на системе: $(uname -a)"
echo "Это важно: бинарный файл будет совместим с Linux-системами, подобными данной."
echo ""

# Проверка наличия необходимых утилит
if ! command -v python3 &> /dev/null; then
    echo "Python 3 не установлен. Установите Python 3 для продолжения."
    exit 1
fi

if ! command -v pip3 &> /dev/null; then
    echo "Pip не установлен. Попытка установки."
    apt install python3-pip
fi

# Установка PyInstaller, если он не установлен
if ! python3 -c "import PyInstaller" &> /dev/null; then
    echo "Установка PyInstaller..."
    pip3 install --upgrade pip setuptools wheel
    pip3 install pyinstaller
fi

# Создание директории для сборки
BUILD_DIR="build_migration"
echo "Создание директории для сборки: $BUILD_DIR"
mkdir -p $BUILD_DIR
cd $BUILD_DIR

# Копирование основного скрипта и зависимостей
echo "Копирование файлов скрипта миграции..."
cp ../main.py .
mkdir -p src
cp -r ../src/* src/

# Проверка наличия конфигурационного файла
CONFIG_FILE="src/config/settings.yaml"
if [ -f "../$CONFIG_FILE" ]; then
    echo "Обнаружен конфигурационный файл. Он будет исключен из бинарного файла."
else
    echo "Предупреждение: конфигурационный файл не найден по пути ../$CONFIG_FILE"
    echo "Он должен быть доступен по этому пути при запуске бинарного файла."
fi

# Создание файла hook-main.py для включения дополнительных модулей
echo "Создание hook-файла для PyInstaller..."
cat > hook-main.py << 'EOF'
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

# Добавление дополнительных модулей, которые могут быть загружены динамически
hiddenimports = [
    'yaml',
    'cryptography',
    'logging'
]

# Добавление всех подмодулей из src
hiddenimports.extend([
    'src',
    'src.logging.logger',
    'src.connection.dfs_connector',
    'src.connection.usb_connector',
    'src.ntfs.ntfs_mounter',
    'src.config.config_loader',
    'src.shortcuts_printers.links_handler',
    'src.shortcuts_printers.shortcut_creator',
    'src.shortcuts_printers.printer_connector',
    'src.migration.direct_migration',
    'src.migration.state_tracker',
    'src.structure.structure_normalizer',
    'src.metrics_monitoring.report',
    'src.metrics_monitoring.report_utils',
    'src.notify.notify'
])

# Сбор всех данных файлов из директории src, кроме конфигурационного файла
datas = []
for module_name in hiddenimports:
    if module_name.startswith('src') and not module_name.startswith('src.config'):
        module_data = collect_data_files(module_name)
        for data_item in module_data:
            if not data_item[0].endswith('settings.yaml'):
                datas.append(data_item)
EOF

# Создание спецификации для PyInstaller
echo "Создание спецификации для PyInstaller..."
cat > migration.spec << 'EOF'
# -*- mode: python ; coding: utf-8 -*-

import sys

# Специфические настройки для Linux
if sys.platform.startswith('linux'):
    # Для Linux может потребоваться включить некоторые библиотеки явно
    binaries = []  # Здесь можно добавить дополнительные бинарные зависимости
else:
    binaries = []

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=binaries,
    datas=[],
    hiddenimports=[],
    hookspath=['.'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['src/config/settings.yaml'],  # Исключаем конфигурационный файл
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Исключение конфигурационного файла из сборки
a.datas = [x for x in a.datas if not x[0].endswith('settings.yaml')]

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='migration',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,  # Оптимизация размера для Linux
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=True,  # Улучшение обработки аргументов командной строки
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
EOF

# Запуск PyInstaller для создания бинарного файла
echo "Запуск PyInstaller для сборки бинарного файла..."
pyinstaller --clean --noconfirm migration.spec

# Проверка результатов
if [ -f "dist/migration" ]; then
    echo "Сборка успешно завершена!"
    echo "Бинарный файл создан: dist/migration"
    
    # Копирование результата в корневую директорию
    cp dist/migration ../
    
    # Установка прав на выполнение
    chmod +x ../migration
    
    echo "Скопировано в корневую директорию проекта и установлены права на выполнение."
    
    # Проверка типа файла для подтверждения, что это Linux-исполняемый файл
    file_info=$(file ../migration)
    echo "Информация о файле: $file_info"
    
    echo ""
    echo "ВНИМАНИЕ: Конфигурационный файл не был включен в бинарный файл."
    echo "Для работы программы файл settings.yaml должен находиться по пути: src/config/settings.yaml"
    echo "относительно директории запуска бинарного файла."
else
    echo "Произошла ошибка при сборке. Проверьте вывод PyInstaller."
    exit 1
fi

cd ..
echo "Очистка временных файлов..."
# Раскомментируйте, если хотите автоматически удалять временные файлы
# rm -rf $BUILD_DIR

echo "Готово! Создан Linux-совместимый бинарный файл: migration"
