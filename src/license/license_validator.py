import base64
import datetime
import json
import logging
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

# Секретный ключ шифрования
ENCRYPTION_KEY = b'secret_key'

def generate_license_key(days_valid=30):
    """
    Генерирует лицензионный ключ с ограничением по времени.
    
    :param days_valid: Количество дней действия лицензии
    :return: Строка с зашифрованным ключом
    """
    # Создаем объект шифрования
    cipher = Fernet(base64.urlsafe_b64encode(ENCRYPTION_KEY[:32]))
    
    # Создаем текущую дату и дату истечения срока
    current_date = datetime.datetime.now()
    expiry_date = current_date + datetime.timedelta(days=days_valid)
    
    # Формируем данные лицензии
    license_data = {
        "created_at": current_date.isoformat(),
        "expires_at": expiry_date.isoformat(),
        "days_valid": days_valid
    }
    
    # Шифруем данные лицензии
    encrypted_data = cipher.encrypt(json.dumps(license_data).encode())
    
    # Возвращаем ключ в виде строки
    return encrypted_data.decode()

def validate_license_key(license_key):
    """
    Проверяет действительность лицензионного ключа.
    
    :param license_key: Лицензионный ключ для проверки
    :return: (bool, str) - (действительна, сообщение)
    """
    try:
        # Создаем объект шифрования
        cipher = Fernet(base64.urlsafe_b64encode(ENCRYPTION_KEY[:32]))
        
        # Расшифровываем ключ
        decrypted_data = cipher.decrypt(license_key.encode())
        license_data = json.loads(decrypted_data.decode())
        
        # Получаем даты
        created_at = datetime.datetime.fromisoformat(license_data["created_at"])
        expires_at = datetime.datetime.fromisoformat(license_data["expires_at"])
        current_date = datetime.datetime.now()
        
        # Проверяем срок действия
        if current_date > expires_at:
            days_expired = (current_date - expires_at).days
            return False, f"Срок действия лицензии истек {days_expired} дней назад"
        
        # Вычисляем оставшееся время
        days_left = (expires_at - current_date).days
        return True, f"Лицензия действительна. Осталось {days_left} дней"
        
    except Exception as e:
        logger.error(f"Ошибка при проверке лицензии: {e}")
        return False, f"Недействительная лицензия: {str(e)}"

def check_license_from_config(config):
    """
    Проверяет лицензию из конфигурационного файла.
    
    :param config: Словарь с конфигурацией
    :return: (bool, str) - (действительна, сообщение)
    """
    license_key = config.get("LICENSE_KEY")
    
    if not license_key:
        return False, "Лицензионный ключ не найден в конфигурации"
    
    return validate_license_key(license_key)