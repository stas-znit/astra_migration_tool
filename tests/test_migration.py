import unittest
import os
from unittest import mock
from src.migration.data_migrator import copy_file, process_special_files, migrate_data, translate_path

class TestDataMigrator(unittest.TestCase):

    @mock.patch('shutil.copy2')
    @mock.patch('os.path.exists')
    @mock.patch('src.data_migrator.calculate_file_hash')
    def test_copy_file(self, mock_hash, mock_exists, mock_copy):
        # Настроим mock для calculate_file_hash и os.path.exists
        mock_exists.side_effect = [False, True]
        mock_hash.side_effect = ['abc', 'xyz']
        
        # Тестируем копирование, если файла нет
        copy_file('source.txt', 'target.txt')
        mock_copy.assert_called_once_with('source.txt', 'target.txt')

        # Сбрасываем mock и тестируем копирование, если файл существует, но хеши разные
        mock_copy.reset_mock()
        copy_file('source.txt', 'target.txt')
        mock_copy.assert_called_once_with('source.txt', 'target.txt')

    @mock.patch('os.makedirs')
    @mock.patch('src.shortcuts_printers.shortcut_creator.create_shortcuts')
    @mock.patch('src.shortcuts_printers.links_handler.parse_links_file')
    def test_process_special_files(self, mock_parse_links, mock_create_shortcuts, mock_makedirs):
        # Тест для обработки links.txt
        process_special_files('links.txt', 'source_links.txt', 'target_links.txt')
        mock_parse_links.assert_called_once_with('source_links.txt')
        mock_create_shortcuts.assert_called_once()

        # Тест для обработки printers.txt
        with mock.patch('src.shortcuts_printers.printer_connector.connect_printers') as mock_connect_printers:
            process_special_files('printers.txt', 'source_printers.txt', 'target_printers.txt')
            mock_connect_printers.assert_called_once_with('source_printers.txt')

    @mock.patch('concurrent.futures.ThreadPoolExecutor')
    @mock.patch('os.walk')
    @mock.patch('os.makedirs')
    def test_migrate_data(self, mock_makedirs, mock_walk, mock_executor):
        # Настроим os.walk для теста
        mock_walk.return_value = [
            ('/source', ['subdir'], ['file1.txt', 'file2.txt', 'links.txt'])
        ]
        executor_instance = mock_executor.return_value.__enter__.return_value
        migrate_data('/source', '/target', exclude_dirs=[], include_files=['.txt'])

        # Проверяем, что файлы были переданы в executor
        executor_instance.submit.assert_any_call(copy_file, '/source/file1.txt', '/target/file1.txt')
        executor_instance.submit.assert_any_call(copy_file, '/source/file2.txt', '/target/file2.txt')

    def test_translate_path(self):
        folder_mapping = {'Documents': 'Документы', 'Downloads': 'Загрузки'}
        result = translate_path('/source/Documents/file.txt', '/source', '/target', folder_mapping)
        expected = '/target/Документы/file.txt'
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
