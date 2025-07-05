#!/usr/bin/env python3
"""
Анализатор ошибок и предупреждений миграции данных.

Извлекает коды ошибок и предупреждений из логов и состояния миграции, 
предоставляет статистику и рекомендации по устранению.

Использование:
    python analyze_migration_errors.py [--log-file LOG_FILE] [--state-file STATE_FILE] [--include-warnings]
"""

import json
import re
import argparse
import sys
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Добавляем путь к модулям проекта
sys.path.append(str(Path(__file__).parent.parent))

try:
    from src.errors.error_codes import MigrationErrorCodes, get_error_by_code, ErrorCategory
except ImportError:
    print("⚠️  Не удалось импортировать модуль кодов ошибок")
    print("Убедитесь, что скрипт запущен из корня проекта")
    sys.exit(1)


class MigrationErrorAnalyzer:
    """Анализатор ошибок и предупреждений миграции"""
    
    def __init__(self, include_warnings: bool = True):
        self.errors_found = []
        self.warnings_found = []
        self.include_warnings = include_warnings
        
        self.error_patterns = {
            # Паттерны для извлечения кодов ошибок из логов
            'error_code': re.compile(r'\[([A-Z]+_\d+)\]'),
            'timestamp': re.compile(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})'),
            'severity_level': re.compile(r'(ERROR|WARNING|CRITICAL|INFO)', re.IGNORECASE),
            'user_context': re.compile(r'пользователь[а|е]?\s+([a-zA-Z0-9._@-]+)', re.IGNORECASE),
            'file_context': re.compile(r'файл[а|е]?\s+([^\s]+\.[a-zA-Z0-9]+)', re.IGNORECASE),
            'function_context': re.compile(r'функци[я|и]\s+([a-zA-Z0-9_]+)', re.IGNORECASE),
            'path_context': re.compile(r'пут[ь|и]\s+([/\\][^\s]+)', re.IGNORECASE)
        }
        
        # Паттерны для стандартных WARNING сообщений Python
        self.warning_patterns = {
            'deprecation': re.compile(r'DeprecationWarning', re.IGNORECASE),
            'resource': re.compile(r'ResourceWarning', re.IGNORECASE),
            'runtime': re.compile(r'RuntimeWarning', re.IGNORECASE),
            'user': re.compile(r'UserWarning', re.IGNORECASE),
            'future': re.compile(r'FutureWarning', re.IGNORECASE)
        }
    
    def analyze_log_file(self, log_file_path: str):
        """Анализирует файл логов на предмет ошибок и предупреждений миграции"""
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            print(f"📋 Анализируем файл логов: {log_file_path}")
            print(f"📄 Всего строк в логе: {len(lines)}")
            
            for line_num, line in enumerate(lines, 1):
                self._parse_log_line(line, line_num)
                
        except FileNotFoundError:
            print(f"❌ Файл логов не найден: {log_file_path}")
        except Exception as e:
            print(f"❌ Ошибка чтения файла логов: {e}")
    
    def analyze_state_file(self, state_file_path: str):
        """Анализирует файл состояния миграции"""
        try:
            with open(state_file_path, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            print(f"📋 Анализируем файл состояния: {state_file_path}")
            
            # Анализируем глобальные ошибки
            global_state = state_data.get('global', {})
            if 'last_error' in global_state and global_state['last_error']:
                self._parse_state_error(global_state['last_error'], 'global')
            
            # Анализируем предупреждения
            if 'last_warning' in global_state and global_state['last_warning']:
                self._parse_state_warning(global_state['last_warning'], 'global')
            
            # Анализируем ошибки по пользователям
            users_state = state_data.get('users', {})
            failed_users = [user for user, status in users_state.items() if status == 'failed']
            warning_users = [user for user, status in users_state.items() if status == 'warning']
            
            if failed_users:
                print(f"👥 Пользователи с ошибками миграции: {len(failed_users)}")
                for user in failed_users:
                    print(f"   • {user}")
            
            if warning_users and self.include_warnings:
                print(f"⚠️  Пользователи с предупреждениями: {len(warning_users)}")
                for user in warning_users:
                    print(f"   • {user}")
            
            # Ищем сводку ошибок, если есть
            if 'error_summary' in global_state:
                self._parse_error_summary(global_state['error_summary'])
                
        except FileNotFoundError:
            print(f"❌ Файл состояния не найден: {state_file_path}")
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON файла состояния: {e}")
        except Exception as e:
            print(f"❌ Ошибка чтения файла состояния: {e}")
    
    def _parse_log_line(self, line: str, line_num: int):
        """Парсит строку лога на предмет ошибок и предупреждений"""
        # Определяем уровень серьезности
        severity_match = self.error_patterns['severity_level'].search(line)
        severity = severity_match.group(1).upper() if severity_match else 'UNKNOWN'
        
        # Ищем код ошибки
        error_match = self.error_patterns['error_code'].search(line)
        
        # Если есть код ошибки - это структурированная ошибка/предупреждение
        if error_match:
            error_code = error_match.group(1)
            
            # Извлекаем дополнительную информацию
            parsed_info = self._extract_context_info(line, line_num, severity)
            parsed_info['code'] = error_code
            parsed_info['source'] = 'log'
            
            # Классифицируем по severity
            if severity in ['ERROR', 'CRITICAL']:
                self.errors_found.append(parsed_info)
            elif severity == 'WARNING' and self.include_warnings:
                self.warnings_found.append(parsed_info)
        
        # Если нет кода ошибки, но есть стандартные WARNING паттерны
        elif self.include_warnings and severity == 'WARNING':
            warning_type = self._identify_warning_type(line)
            if warning_type:
                parsed_info = self._extract_context_info(line, line_num, severity)
                parsed_info['code'] = f'PYTHON_{warning_type.upper()}'
                parsed_info['warning_type'] = warning_type
                parsed_info['source'] = 'log'
                self.warnings_found.append(parsed_info)
    
    def _extract_context_info(self, line: str, line_num: int, severity: str) -> Dict[str, Any]:
        """Извлекает контекстную информацию из строки лога"""
        timestamp_match = self.error_patterns['timestamp'].search(line)
        user_match = self.error_patterns['user_context'].search(line)
        file_match = self.error_patterns['file_context'].search(line)
        function_match = self.error_patterns['function_context'].search(line)
        path_match = self.error_patterns['path_context'].search(line)
        
        return {
            'line_number': line_num,
            'severity': severity,
            'timestamp': timestamp_match.group(1) if timestamp_match else None,
            'user': user_match.group(1) if user_match else None,
            'file': file_match.group(1) if file_match else None,
            'function': function_match.group(1) if function_match else None,
            'path': path_match.group(1) if path_match else None,
            'full_line': line.strip()
        }
    
    def _identify_warning_type(self, line: str) -> Optional[str]:
        """Определяет тип предупреждения Python"""
        for warning_type, pattern in self.warning_patterns.items():
            if pattern.search(line):
                return warning_type
        return None
    
    def _parse_state_error(self, error_data: Dict[str, Any], context: str):
        """Парсит ошибку из файла состояния"""
        error_info = {
            'code': error_data.get('code', 'UNKNOWN'),
            'category': error_data.get('category', 'UNKNOWN'),
            'description': error_data.get('description', ''),
            'details': error_data.get('details', ''),
            'severity': error_data.get('severity', 'ERROR'),
            'timestamp': error_data.get('timestamp', ''),
            'context': context,
            'source': 'state'
        }
        
        if 'exception' in error_data:
            error_info['exception_type'] = error_data['exception'].get('type', '')
            error_info['exception_message'] = error_data['exception'].get('message', '')
        
        self.errors_found.append(error_info)
    
    def _parse_state_warning(self, warning_data: Dict[str, Any], context: str):
        """Парсит предупреждение из файла состояния"""
        if not self.include_warnings:
            return
            
        warning_info = {
            'code': warning_data.get('code', 'UNKNOWN'),
            'category': warning_data.get('category', 'UNKNOWN'),
            'description': warning_data.get('description', ''),
            'details': warning_data.get('details', ''),
            'severity': 'WARNING',
            'timestamp': warning_data.get('timestamp', ''),
            'context': context,
            'source': 'state'
        }
        
        self.warnings_found.append(warning_info)
    
    def _parse_error_summary(self, summary_data: Dict[str, Any]):
        """Парсит сводку ошибок"""
        print(f"\n📊 Сводка из состояния:")
        print(f"   Всего ошибок: {summary_data.get('total_errors', 0)}")
        
        by_category = summary_data.get('by_category', {})
        for category, count in by_category.items():
            if count > 0:
                print(f"   {category}: {count}")
    
    def generate_report(self):
        """Генерирует подробный отчет об ошибках и предупреждениях"""
        total_issues = len(self.errors_found) + len(self.warnings_found)
        
        if total_issues == 0:
            print("\n✅ Ошибки и предупреждения миграции не найдены!")
            return
        
        print(f"\n📊 АНАЛИЗ ОШИБОК И ПРЕДУПРЕЖДЕНИЙ МИГРАЦИИ")
        print("=" * 60)
        print(f"Найдено ошибок: {len(self.errors_found)}")
        if self.include_warnings:
            print(f"Найдено предупреждений: {len(self.warnings_found)}")
        
        # Анализируем ошибки
        if self.errors_found:
            self._analyze_errors()
        
        # Анализируем предупреждения
        if self.warnings_found and self.include_warnings:
            self._analyze_warnings()
        
        # Временная статистика
        self._analyze_timeline()
        
        # Проблемные пользователи
        self._analyze_problematic_users()
        
        # Рекомендации
        self._generate_recommendations()
    
    def _analyze_errors(self):
        """Анализирует ошибки"""
        print(f"\n❌ АНАЛИЗ ОШИБОК:")
        print("-" * 30)
        
        # Статистика по кодам ошибок
        error_codes = [err['code'] for err in self.errors_found]
        code_counts = Counter(error_codes)
        
        print(f"🔢 Статистика по кодам ошибок:")
        for code, count in code_counts.most_common():
            error_def = get_error_by_code(code)
            if error_def:
                print(f"   {code}: {count} раз - {error_def.description}")
            else:
                print(f"   {code}: {count} раз - (неизвестный код)")
        
        # Статистика по категориям
        categories = defaultdict(int)
        severity_counts = defaultdict(int)
        
        for error in self.errors_found:
            error_def = get_error_by_code(error['code'])
            if error_def:
                categories[error_def.category.value] += 1
            else:
                categories['UNKNOWN'] += 1
            severity_counts[error.get('severity', 'ERROR')] += 1
        
        print(f"\n📁 Статистика по категориям ошибок:")
        for category, count in sorted(categories.items()):
            print(f"   {category}: {count}")
        
        print(f"\n🚨 Статистика по уровню критичности:")
        for severity, count in sorted(severity_counts.items()):
            icon = "🔴" if severity == "CRITICAL" else "🟠" if severity == "ERROR" else "🟡"
            print(f"   {icon} {severity}: {count}")
    
    def _analyze_warnings(self):
        """Анализирует предупреждения"""
        print(f"\n⚠️  АНАЛИЗ ПРЕДУПРЕЖДЕНИЙ:")
        print("-" * 30)
        
        # Статистика по кодам предупреждений
        warning_codes = [warn['code'] for warn in self.warnings_found]
        code_counts = Counter(warning_codes)
        
        print(f"🔢 Статистика по кодам предупреждений:")
        for code, count in code_counts.most_common():
            if code.startswith('PYTHON_'):
                warning_type = code.replace('PYTHON_', '').lower()
                print(f"   {code}: {count} раз - {warning_type} предупреждение Python")
            else:
                warning_def = get_error_by_code(code)
                if warning_def:
                    print(f"   {code}: {count} раз - {warning_def.description}")
                else:
                    print(f"   {code}: {count} раз - (неизвестный код)")
        
        # Группировка по типам
        warning_types = defaultdict(int)
        for warning in self.warnings_found:
            if 'warning_type' in warning:
                warning_types[warning['warning_type']] += 1
            else:
                warning_types['migration'] += 1
        
        if warning_types:
            print(f"\n📋 Типы предупреждений:")
            for warn_type, count in sorted(warning_types.items()):
                print(f"   {warn_type}: {count}")
    
    def _analyze_timeline(self):
        """Анализирует временную статистику"""
        all_issues = self.errors_found + (self.warnings_found if self.include_warnings else [])
        timestamps = [issue.get('timestamp') for issue in all_issues if issue.get('timestamp')]
        
        if timestamps:
            print(f"\n⏰ Временной диапазон проблем:")
            print(f"   Первая: {min(timestamps)}")
            print(f"   Последняя: {max(timestamps)}")
            
            # Группировка по часам для выявления пиков
            hour_counts = defaultdict(int)
            for ts in timestamps:
                try:
                    hour = ts.split('T')[1][:2] if 'T' in ts else ts.split(' ')[1][:2]
                    hour_counts[hour] += 1
                except:
                    continue
            
            if hour_counts:
                peak_hour = max(hour_counts.items(), key=lambda x: x[1])
                print(f"   Пик проблем: {peak_hour[0]}:00 ({peak_hour[1]} событий)")
    
    def _analyze_problematic_users(self):
        """Анализирует проблемных пользователей"""
        all_issues = self.errors_found + (self.warnings_found if self.include_warnings else [])
        users = [issue.get('user') for issue in all_issues if issue.get('user')]
        
        if users:
            user_counts = Counter(users)
            print(f"\n👥 Пользователи с наибольшим количеством проблем:")
            for user, count in user_counts.most_common(5):
                user_errors = len([e for e in self.errors_found if e.get('user') == user])
                user_warnings = len([w for w in self.warnings_found if w.get('user') == user]) if self.include_warnings else 0
                
                status = "❌" if user_errors > 0 else "⚠️"
                print(f"   {status} {user}: {count} проблем (ошибок: {user_errors}, предупреждений: {user_warnings})")
    
    def _generate_recommendations(self):
        """Генерирует рекомендации по устранению ошибок и предупреждений"""
        print(f"\n💡 РЕКОМЕНДАЦИИ ПО УСТРАНЕНИЮ:")
        print("-" * 40)
        
        # Анализируем наиболее частые ошибки
        if self.errors_found:
            error_codes = [err['code'] for err in self.errors_found]
            top_errors = Counter(error_codes).most_common(3)
            
            print(f"🔧 Критические ошибки для устранения:")
            for code, count in top_errors:
                error_def = get_error_by_code(code)
                if error_def:
                    print(f"\n   {code} (встречается {count} раз):")
                    print(f"   Проблема: {error_def.description}")
                    print(f"   Решение: {error_def.solution}")
        
        # Анализируем предупреждения
        if self.warnings_found and self.include_warnings:
            warning_codes = [warn['code'] for warn in self.warnings_found]
            top_warnings = Counter(warning_codes).most_common(3)
            
            print(f"\n⚠️  Предупреждения для внимания:")
            for code, count in top_warnings:
                if code.startswith('PYTHON_'):
                    warning_type = code.replace('PYTHON_', '').lower()
                    print(f"\n   {code} (встречается {count} раз):")
                    print(f"   Рекомендация: Обновите код для устранения {warning_type} предупреждений")
                else:
                    warning_def = get_error_by_code(code)
                    if warning_def:
                        print(f"\n   {code} (встречается {count} раз):")
                        print(f"   Проблема: {warning_def.description}")
                        print(f"   Решение: {warning_def.solution}")
        
        # Общие рекомендации
        categories = defaultdict(int)
        for error in self.errors_found:
            error_def = get_error_by_code(error['code'])
            if error_def:
                categories[error_def.category.value] += 1
        
        self._generate_category_recommendations(categories)
        
        print(f"\n🔄 Общие рекомендации:")
        print(f"   • Миграция может быть продолжена с места остановки")
        print(f"   • Исправьте критические ошибки перед продолжением")
        if self.warnings_found:
            print(f"   • Предупреждения не блокируют миграцию, но требуют внимания")
        print(f"   • Увеличьте уровень логирования для детальной диагностики")
    
    def _generate_category_recommendations(self, categories: Dict[str, int]):
        """Генерирует рекомендации по категориям ошибок"""
        if categories.get('MOUNT', 0) > 0:
            print(f"\n🔌 Проблемы монтирования ({categories['MOUNT']} ошибок):")
            print(f"   • Проверьте сетевое соединение")
            print(f"   • Убедитесь в правильности credentials")
            print(f"   • Увеличьте MOUNT_ATTEMPTS в конфигурации")
        
        if categories.get('TARGET', 0) > 0:
            print(f"\n💾 Проблемы с целевой системой ({categories['TARGET']} ошибок):")
            print(f"   • Проверьте свободное место на диске")
            print(f"   • Убедитесь в наличии прав доступа")
            print(f"   • Запустите скрипт с правами администратора")
        
        if categories.get('COPY', 0) > 0:
            print(f"\n📁 Проблемы копирования ({categories['COPY']} ошибок):")
            print(f"   • Проверьте целостность исходных файлов")
            print(f"   • Освободите место на целевом диске")
            print(f"   • Закройте приложения, блокирующие файлы")
        
        if categories.get('USER', 0) > 0:
            print(f"\n👤 Проблемы с пользователями ({categories['USER']} ошибок):")
            print(f"   • Проверьте права на создание пользователей")
            print(f"   • Убедитесь в уникальности имен пользователей")
            print(f"   • Проверьте корректность доменных настроек")
        
        if categories.get('NETWORK', 0) > 0:
            print(f"\n🌐 Сетевые проблемы ({categories['NETWORK']} ошибок):")
            print(f"   • Проверьте стабильность сетевого соединения")
            print(f"   • Увеличьте таймауты в конфигурации")
            print(f"   • Рассмотрите возможность локального копирования")


def main():
    parser = argparse.ArgumentParser(description="Анализатор ошибок и предупреждений миграции данных")
    parser.add_argument('--log-file', '-l', 
                       default='/var/log/migration_log_*.log',
                       help='Путь к файлу логов миграции')
    parser.add_argument('--state-file', '-s',
                       default='/var/lib/migration-service/state.json',
                       help='Путь к файлу состояния миграции')
    parser.add_argument('--no-warnings', action='store_true',
                       help='Не анализировать предупреждения')
    parser.add_argument('--summary-only', action='store_true',
                       help='Показать только краткую сводку')
    parser.add_argument('--export-json', 
                       help='Экспортировать результаты в JSON файл')
    
    args = parser.parse_args()
    
    analyzer = MigrationErrorAnalyzer(include_warnings=not args.no_warnings)
    
    print("🔍 АНАЛИЗАТОР ОШИБОК И ПРЕДУПРЕЖДЕНИЙ МИГРАЦИИ")
    print("=" * 50)
    
    # Анализируем файлы
    if Path(args.log_file).exists():
        analyzer.analyze_log_file(args.log_file)
    else:
        # Ищем файлы логов по маске
        log_files = list(Path('/var/log').glob('migration_log_*.log'))
        if log_files:
            latest_log = max(log_files, key=lambda p: p.stat().st_mtime)
            analyzer.analyze_log_file(str(latest_log))
        else:
            print(f"⚠️  Файлы логов не найдены")
    
    if Path(args.state_file).exists():
        analyzer.analyze_state_file(args.state_file)
    else:
        print(f"⚠️  Файл состояния не найден: {args.state_file}")
    
    # Генерируем отчет
    analyzer.generate_report()
    
    # Экспорт в JSON если запрошен
    if args.export_json:
        export_data = {
            'errors': analyzer.errors_found,
            'warnings': analyzer.warnings_found if analyzer.include_warnings else [],
            'analysis_timestamp': datetime.now().isoformat(),
            'total_errors': len(analyzer.errors_found),
            'total_warnings': len(analyzer.warnings_found) if analyzer.include_warnings else 0
        }
        
        with open(args.export_json, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Результаты экспортированы в: {args.export_json}")


if __name__ == "__main__":
    main()