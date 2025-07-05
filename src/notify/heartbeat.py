import requests
import time
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from src.config.config_loader import load_config, get_hostname
from ..logging.logger import setup_logger

logger = setup_logger() or logging.getLogger()

class Heartbeat:
    def __init__(self):
        self.config = load_config()
        self.base_url = self.config.get('MONITORING_BASE_URL') + "/api"
        self.hostname = get_hostname()
        self.started_at = self.config.get('STARTED_AT', time.time())

    def send_heartbeat(self, status: str, current_step: str):
        """
        Отправка heartbeat в сервис мониторинга
        """
        try:
            if not self.base_url:
                logger.debug("base_url не настроен, пропускаем отправку heartbeat")
                return True
                
            request_url = f"{self.base_url}/heartbeat"
            request_params = {
                "hostname": self.hostname, 
                "status": status, 
                "current_step": current_step, 
                "elapsed": time.time() - self.started_at
            }
            response = requests.get(request_url, params=request_params, timeout=10)
            
            if response.status_code == 200:
                logger.debug(f"Heartbeat отправлен: {status} - {current_step}")
                return True
            else:
                logger.warning(f"Ошибка отправки heartbeat: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Ошибка сети при отправке heartbeat: {e}")
            return False
        except Exception as e:
            logger.warning(f"Неожиданная ошибка при отправке heartbeat: {e}")
            return False

    def send_report(self, report_data: dict):
        """
        Отправка отчета в сервис мониторинга
        """
        try:
            if not self.base_url:
                logger.debug("base_url не настроен, пропускаем отправку отчета")
                return True
                
            headers = {
                'Content-Type': 'application/json',
                'X-Hostname': self.hostname
            }
            
            report_url = f"{self.base_url}/report"
            
            # Логируем данные для отладки
            logger.debug(f"URL отправки отчета: {report_url}")
            logger.debug(f"Заголовки: {headers}")
            logger.debug(f"Отправка отчета: {json.dumps(report_data, indent=2, ensure_ascii=False)}")
            
            response = requests.post(
                report_url,
                json=report_data,
                headers=headers,
                timeout=30
            )
            
            logger.debug(f"Статус ответа: {response.status_code}")
            
            if response.status_code == 200:
                logger.info(f"Отчет успешно отправлен")
                return True
            else:
                logger.error(f"Ошибка отправки отчета: HTTP {response.status_code}")
                logger.error(f"URL запроса: {report_url}")
                logger.error(f"Заголовки запроса: {headers}")
                logger.error(f"Тело запроса: {json.dumps(report_data, ensure_ascii=False)}")
                try:
                    error_response = response.json()
                    logger.error(f"Ответ сервера (JSON): {json.dumps(error_response, indent=2, ensure_ascii=False)}")
                except:
                    logger.error(f"Ответ сервера (текст): {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при отправке отчета: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке отчета: {e}")
            return False

    def create_user_report(self, 
                          username: str,
                          source_dir: str,
                          target_dir: str,
                          total_files: int,
                          total_size: str,
                          target_size: str,
                          files_copied: int,
                          copy_errors: List[str],
                          files_verified: int,
                          discrepancies: List[str],
                          start_time: datetime,
                          end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Создание отчета о миграции пользователя в формате ReportData согласно API схеме
        """
        if end_time is None:
            end_time = datetime.now()
        
        # Валидируем и преобразуем типы данных
        try:
            total_files = int(total_files) if total_files is not None else 0
            files_copied = int(files_copied) if files_copied is not None else 0
            files_verified = int(files_verified) if files_verified is not None else 0
        except (ValueError, TypeError):
            logger.warning("Ошибка преобразования числовых полей в отчете")
            total_files = 0
            files_copied = 0
            files_verified = 0
        
        if not isinstance(copy_errors, list):
            copy_errors = [str(copy_errors)] if copy_errors else []
        if not isinstance(discrepancies, list):
            discrepancies = [str(discrepancies)] if discrepancies else []
            
        def format_datetime(dt: datetime) -> str:
            """Форматирует datetime в RFC3339 формат для API"""
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
        # Формируем отчет согласно схеме API
        report_data = {
            "username": str(username),
            "source_dir": str(source_dir),
            "target_dir": str(target_dir),
            "total_files": total_files,
            "total_size": str(total_size),
            "target_size": str(target_size),
            "files_copied": files_copied,
            "copy_errors": copy_errors,
            "files_verified": files_verified,
            "discrepancies": discrepancies,
            "start_time": format_datetime(start_time),
            "end_time": format_datetime(end_time)
        }
        
        return report_data
