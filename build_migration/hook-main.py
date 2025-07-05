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
