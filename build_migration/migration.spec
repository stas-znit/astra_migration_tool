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
