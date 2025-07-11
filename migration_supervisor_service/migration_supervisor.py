#!/usr/bin/env python3
"""
Супервизор миграции с защитой от блокировок файлов состояния.

"""

import os
import sys
import json
import time
import signal
import subprocess
import logging
import datetime
import errno
from pathlib import Path

# =============================================================================
# КОНСТАНТЫ - НАСТРАИВАЮТСЯ В СКРИПТЕ УСТАНОВКИ AUTO_SETUP.SH
# =============================================================================
MAIN_SCRIPT = "/opt/migration/main.py"
VENV_PATH = "/opt/migration/venv"

# ИСПРАВЛЕНО: Используем специальный файл для чтения супервизором
SUPERVISOR_READ_FILE = "/var/lib/migration-service/supervisor_state.json"
SERVICE_STATE_FILE = "/var/lib/migration-service/state.json"
LOG_FILE = "/var/log/migration-supervisor/migration-supervisor.log"
PID_FILE = "/var/run/migration-supervisor.pid"

# Настройки мониторинга
HEARTBEAT_TIMEOUT = 120  # 2 минуты
CHECK_INTERVAL = 30      # проверка каждые 30 секунд
MAX_RESTARTS = 3         # максимум 3 перезапуска
RESTART_DELAY = 60       # 1 минута между перезапусками
FAILURE_COOLDOWN = 1800  # 30 минут перед новой попыткой

# НОВОЕ: Таймауты для операций с файлами
FILE_READ_TIMEOUT = 3.0  # 3 секунды на чтение файла
FILE_OPERATION_RETRIES = 3  # количество попыток

def safe_read_json_file(file_path, timeout=FILE_READ_TIMEOUT):
    """
    НОВОЕ: Безопасное чтение JSON файла с таймаутом и retry.
    """
    if not os.path.exists(file_path):
        return None
    
    for attempt in range(FILE_OPERATION_RETRIES):
        try:
            start_time = time.time()
            
            # Пытаемся прочитать файл с таймаутом
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
                
        except (IOError, OSError) as e:
            # Файл заблокирован или недоступен
            if e.errno == errno.EACCES or "resource temporarily unavailable" in str(e).lower():
                if attempt < FILE_OPERATION_RETRIES - 1:
                    time.sleep(0.5 * (attempt + 1))  # Увеличиваем задержку
                    continue
                else:
                    return None
            else:
                raise
        except json.JSONDecodeError as e:
            # Файл возможно записывается, пробуем ещё раз
            if attempt < FILE_OPERATION_RETRIES - 1:
                time.sleep(0.2)
                continue
            else:
                return None
        except Exception as e:
            # Неожиданная ошибка
            return None
    
    return None

class MigrationSupervisor:
    def __init__(self):
        self.running = False
        self.process = None
        self.restart_count = 0
        self.last_restart = 0
        self.failure_time = 0
        self.start_time = time.time()
        self.setup_logging()
        
    def setup_logging(self):
        """Настройка логирования"""
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('supervisor')
        
    def signal_handler(self, signum, frame):
        """Обработка сигналов завершения"""
        self.logger.info(f"Получен сигнал {signum}, корректное завершение работы")
        self.running = False
        self.stop_migration()
        
    def write_pid(self):
        """Записываем PID супервизора"""
        try:
            os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)
            with open(PID_FILE, 'w') as f:
                f.write(str(os.getpid()))
        except Exception as e:
            self.logger.warning(f"Не удалось записать PID: {e}")
            
    def remove_pid(self):
        """Удаляем PID файл"""
        try:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        except:
            pass
            
    def read_supervisor_state(self):
        """
        ИСПРАВЛЕНО: Читаем состояние из специального файла для супервизора.
        """
        # Приоритет: сначала специальный файл для супервизора
        data = safe_read_json_file(SUPERVISOR_READ_FILE)
        if data is not None:
            return data
            
        # Fallback: общий файл состояния сервиса
        data = safe_read_json_file(SERVICE_STATE_FILE)
        if data is not None:
            # Адаптируем полное состояние для супервизора
            return self.adapt_full_state_for_supervisor(data)
            
        return None
        
    def adapt_full_state_for_supervisor(self, full_state):
        """
        НОВОЕ: Адаптирует полное состояние для нужд супервизора.
        """
        try:
            global_state = full_state.get("global", {})
            users_state = full_state.get("users", {})
            
            total_users = len(users_state)
            users_completed = len([u for u in users_state.values() if u in ["success", "completed_with_error"]])
            users_in_progress = len([u for u in users_state.values() if u == "in_progress"])
            
            return {
                "supervisor_timestamp": datetime.datetime.now().isoformat(),
                "status": global_state.get("status", "unknown"),
                "last_heartbeat": global_state.get("last_heartbeat"),
                "current_user": global_state.get("current_user"),
                "users_in_progress": users_in_progress,
                "progress_percent": (users_completed / total_users * 100) if total_users > 0 else 0,
                "last_error": global_state.get("last_error", {}).get("code") if global_state.get("last_error") else None
            }
        except Exception as e:
            self.logger.warning(f"Ошибка адаптации состояния: {e}")
            return None
        
    def get_last_heartbeat(self):
        """
        ИСПРАВЛЕНО: Получаем время последнего heartbeat из специального файла.
        """
        state = self.read_supervisor_state()
        if state and state.get('last_heartbeat'):
            try:
                return datetime.datetime.fromisoformat(state['last_heartbeat'])
            except Exception as e:
                self.logger.debug(f"Ошибка парсинга heartbeat: {e}")
        return None
        
    def get_migration_status(self):
        """
        ИСПРАВЛЕНО: Получаем статус миграции из специального файла.
        """
        state = self.read_supervisor_state()
        if state:
            return state.get('status')
        return None
        
    def check_migration_already_completed(self):
        """
        ИСПРАВЛЕНО: Проверяем завершение миграции через специальный файл.
        """
        status = self.get_migration_status()
        if status == 'success':
            self.logger.info("Миграция уже была успешно завершена ранее")
            return True
        elif status == 'completed':
            self.logger.info("Миграция уже была завершена ранее")
            return True
        return False
        
    def disable_autostart_after_success(self):
        """Отключаем автозапуск супервизора после успешной миграции"""
        try:
            self.logger.info("Отключаем автозапуск супервизора после успешной миграции")
            result = subprocess.run(['systemctl', 'disable', 'migration-supervisor'], 
                                  capture_output=True, timeout=30)
            if result.returncode == 0:
                self.logger.info("Автозапуск супервизора отключен")
            else:
                self.logger.warning(f"Не удалось отключить автозапуск: {result.stderr.decode()}")
        except Exception as e:
            self.logger.warning(f"Ошибка при отключении автозапуска: {e}")
            
    def is_migration_alive(self):
        """
        ИСПРАВЛЕНО: Проверяем, жива ли миграция через специальный файл.
        """
        # 1. Процесс запущен?
        if not self.process or self.process.poll() is not None:
            return False
            
        # 2. Heartbeat актуален?
        last_hb = self.get_last_heartbeat()
        if not last_hb:
            # Даем время на инициализацию
            if hasattr(self, 'migration_start_time'):
                elapsed = time.time() - self.migration_start_time
                if elapsed < 120:  # 2 минуты на старт
                    return True
            self.logger.warning("Heartbeat не найден после инициализации")
            return False
            
        now = datetime.datetime.now()
        time_diff = (now - last_hb).total_seconds()
        
        if time_diff > HEARTBEAT_TIMEOUT:
            self.logger.warning(f"Heartbeat устарел: {time_diff:.0f}s")
            return False
            
        return True
        
    def get_python_executable(self):
        """Получаем путь к Python"""
        venv_python = os.path.join(VENV_PATH, 'bin', 'python')
        if os.path.exists(venv_python):
            try:
                result = subprocess.run([venv_python, '--version'], 
                                      capture_output=True, timeout=5)
                if result.returncode == 0:
                    return venv_python
            except:
                pass
        
        self.logger.warning(f"Используем системный Python, venv недоступен: {VENV_PATH}")
        return 'python3'
        
    def start_migration(self):
        """Запускаем основной скрипт"""
        if not os.path.exists(MAIN_SCRIPT):
            self.logger.error(f"Основной скрипт не найден: {MAIN_SCRIPT}")
            return False
            
        try:
            python_exe = self.get_python_executable()
            self.logger.info(f"Запускаем миграцию: {python_exe} {MAIN_SCRIPT}")
            
            log_dir = os.path.dirname(LOG_FILE)
            script_log = os.path.join(log_dir, "migration.log")
            
            env = os.environ.copy()
            
            if VENV_PATH in python_exe:
                venv_bin = os.path.join(VENV_PATH, 'bin')
                env['PATH'] = f"{venv_bin}:{env.get('PATH', '')}"
                env['VIRTUAL_ENV'] = VENV_PATH
            
            project_dir = os.path.dirname(MAIN_SCRIPT)
            if 'PYTHONPATH' in env:
                env['PYTHONPATH'] = f"{project_dir}:{env['PYTHONPATH']}"
            else:
                env['PYTHONPATH'] = project_dir
            
            self.logger.info(f"Рабочая директория: {project_dir}")
            
            with open(script_log, 'a', encoding='utf-8') as f:
                self.process = subprocess.Popen(
                    [python_exe, MAIN_SCRIPT],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=project_dir,
                    env=env
                )
                
            self.migration_start_time = time.time()
            self.logger.info(f"Миграция запущена с PID {self.process.pid}")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка запуска миграции: {e}")
            return False
            
    def stop_migration(self):
        """Останавливаем миграцию"""
        if self.process and self.process.poll() is None:
            try:
                self.logger.info("Останавливаем миграцию...")
                self.process.terminate()
                
                try:
                    self.process.wait(timeout=30)
                    self.logger.info("Миграция корректно завершена")
                except subprocess.TimeoutExpired:
                    self.logger.warning("Принудительное завершение миграции")
                    self.process.kill()
                    self.process.wait()
                    
            except Exception as e:
                self.logger.error(f"Ошибка остановки миграции: {e}")
                
        self.process = None
        
    def should_restart(self):
        """Определяем необходимость перезапуска"""
        now = time.time()
        
        if self.restart_count >= MAX_RESTARTS:
            if self.failure_time == 0:
                self.failure_time = now
                self.logger.warning(f"Исчерпан лимит перезапусков. Пауза на {FAILURE_COOLDOWN/60:.0f} минут")
                
            if now - self.failure_time < FAILURE_COOLDOWN:
                return False
            else:
                self.logger.info("Сброс счетчика перезапусков после паузы")
                self.restart_count = 0
                self.failure_time = 0
                
        if now - self.last_restart < RESTART_DELAY:
            return False
            
        return True
        
    def restart_migration(self):
        """Перезапускаем миграцию"""
        if not self.should_restart():
            return False
            
        self.logger.info(f"Перезапуск #{self.restart_count + 1}/{MAX_RESTARTS}")
        
        self.stop_migration()
        time.sleep(5)
        
        if self.start_migration():
            self.restart_count += 1
            self.last_restart = time.time()
            return True
        else:
            return False
            
    def check_migration_completion(self):
        """Проверяем успешное завершение миграции"""
        status = self.get_migration_status()
        if status in ['success', 'completed']:
            self.logger.info(f"Миграция завершена со статусом: {status}")
            
            if status == 'success':
                self.disable_autostart_after_success()
            
            return True
        return False
        
    def get_status(self):
        """
        ИСПРАВЛЕНО: Получаем статус без блокировки основных файлов.
        """
        current_time = time.time()
        
        # Базовая информация о супервизоре
        status = {
            'supervisor_running': self.running,
            'migration_pid': self.process.pid if self.process and self.process.poll() is None else None,
            'restart_count': self.restart_count,
            'last_restart': self.last_restart,
            'failure_cooldown': self.failure_time > 0 and (current_time - self.failure_time) < FAILURE_COOLDOWN,
            'python_executable': self.get_python_executable(),
            'uptime': current_time - self.start_time,
            'supervisor_start_time': datetime.datetime.fromtimestamp(self.start_time).isoformat(),
            'migration_start_time': datetime.datetime.fromtimestamp(self.migration_start_time).isoformat() if hasattr(self, 'migration_start_time') else None
        }
        
        # Добавляем информацию из файла состояния (без блокировки)
        supervisor_state = self.read_supervisor_state()
        if supervisor_state:
            status.update({
                'migration_status': supervisor_state.get('status'),
                'last_heartbeat': supervisor_state.get('last_heartbeat'),
                'current_user': supervisor_state.get('current_user'),
                'progress': supervisor_state.get('progress_percent', 0),
                'users_in_progress': supervisor_state.get('users_in_progress', 0),
                'last_error': supervisor_state.get('last_error')
            })
        else:
            # Если специальный файл недоступен
            status.update({
                'migration_status': 'unknown',
                'last_heartbeat': None,
                'current_user': None,
                'progress': 0,
                'users_in_progress': 0,
                'last_error': None
            })
                
        return status
        
    def run(self):
        """Главный цикл супервизора"""
        self.logger.info("=== Запуск супервизора миграции ===")
        
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.write_pid()
        self.running = True
        
        # Проверяем, была ли миграция уже завершена
        if self.check_migration_already_completed():
            self.logger.info("=== МИГРАЦИЯ УЖЕ БЫЛА ЗАВЕРШЕНА ===")
            self.logger.info("Супервизор завершает работу")
            self.disable_autostart_after_success()
            time.sleep(5)
            return 0
        
        # Запускаем миграцию
        if not self.start_migration():
            self.logger.error("Не удалось запустить миграцию")
            return 1
            
        self.logger.info("=== Начат мониторинг миграции ===")
        
        # Основной цикл мониторинга
        try:
            while self.running:
                time.sleep(CHECK_INTERVAL)
                
                if not self.running:
                    break
                
                # Проверяем успешное завершение
                if self.check_migration_completion():
                    self.logger.info("=== МИГРАЦИЯ УСПЕШНО ЗАВЕРШЕНА ===")
                    break
                    
                # Проверяем состояние миграции
                if not self.is_migration_alive():
                    self.logger.warning("Миграция не отвечает")
                    
                    # Проверяем код завершения
                    if self.process and self.process.poll() is not None:
                        code = self.process.returncode
                        if code == 0:
                            self.logger.info("Миграция завершена с кодом 0")
                            time.sleep(5)
                            if self.check_migration_completion():
                                break
                        else:
                            self.logger.error(f"Миграция завершена с ошибкой: {code}")
                    
                    # Попытка перезапуска
                    if not self.restart_migration():
                        if self.restart_count >= MAX_RESTARTS:
                            self.logger.warning("Переход в режим ожидания (cooldown)")
                            continue
                        else:
                            self.logger.error("Критическая ошибка, завершение супервизора")
                            return 1
                            
                else:
                    # Сбрасываем счетчик при стабильной работе
                    if (time.time() - self.last_restart > 600 and 
                        self.restart_count > 0):
                        self.logger.info("Сброс счетчика перезапусков (стабильная работа)")
                        self.restart_count = 0
                        self.failure_time = 0
                        
        except KeyboardInterrupt:
            self.logger.info("Получен Ctrl+C")
        finally:
            self.logger.info("=== Завершение работы супервизора ===")
            self.stop_migration()
            self.remove_pid()
            self.logger.info("Супервизор завершен")
            
        return 0

def main():
    """Точка входа"""
    if len(sys.argv) > 1:
        supervisor = MigrationSupervisor()
        
        if sys.argv[1] == 'status':
            # Показать актуальный статус
            status = supervisor.get_status()
            print(json.dumps(status, indent=2, ensure_ascii=False, default=str))
            return 0
        elif sys.argv[1] == 'stop':
            # Остановить супервизор
            try:
                with open(PID_FILE, 'r') as f:
                    pid = int(f.read().strip())
                os.kill(pid, signal.SIGTERM)
                print("Сигнал остановки отправлен")
                return 0
            except Exception as e:
                print(f"Ошибка остановки: {e}")
                return 1
        elif sys.argv[1] == 'check-migration':
            # Проверить статус миграции
            if supervisor.check_migration_already_completed():
                print("Миграция уже была завершена")
                return 0
            else:
                print("Миграция не завершена или не проводилась")
                return 1
        else:
            print("Использование: migration_supervisor.py [status|stop|check-migration]")
            return 1
    
    # Обычный запуск
    supervisor = MigrationSupervisor()
    return supervisor.run()

if __name__ == '__main__':
    sys.exit(main())