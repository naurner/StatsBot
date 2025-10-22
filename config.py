"""Конфигурация приложения."""
import os
import logging
from pathlib import Path
from typing import Optional
import json


logger = logging.getLogger(__name__)


def read_token_from_file(path: Path) -> Optional[str]:
    """Читает токен из файла."""
    try:
        if path.exists():
            token = path.read_text(encoding="utf-8").strip()
            return token if token else None
    except Exception as e:
        logger.debug(f"Не удалось прочитать токен из файла {path}: {e}")
    return None


# Пути к файлам
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
TOKEN_FILE = BASE_DIR / "telegram_token.txt"
CONFIG_FILE = DATA_DIR / 'config.json'
EXCEL_PATH = DATA_DIR / "model.xlsx"

# Создание директории для данных, если её нет
DATA_DIR.mkdir(exist_ok=True)

# Токен бота
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or read_token_from_file(TOKEN_FILE)

# Параметры
EARNINGS_PERCENTAGE = 0.15  # 15% от заработка

# Названия листов в Excel
EARNINGS_SHEET = "заработок"
CUSTOMER_INFO_SHEET = "инфо о покупателе"


class ConfigManager:
    """Менеджер конфигурации для хранения дат и настроек."""

    def __init__(self, config_path: Path = CONFIG_FILE):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Загружает конфигурацию из JSON файла."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка при загрузке конфигурации: {e}")
                return self._default_config()
        return self._default_config()

    def _default_config(self) -> dict:
        """Возвращает конфигурацию по умолчанию."""
        return {
            'dates_history': [],
            'last_date': None,
            'settings': {
                'earnings_percentage': EARNINGS_PERCENTAGE
            }
        }

    def save(self):
        """Сохраняет конфигурацию в JSON файл."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2, default=str)
            logger.debug(f"Конфигурация сохранена в {self.config_path}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении конфигурации: {e}")

    def get_dates_history(self) -> list:
        """Возвращает историю дат выплат."""
        return self.config.get('dates_history', [])

    def add_dates_to_history(self, date_till: str, date_of_pay: str):
        """Добавляет новые даты в историю."""
        if 'dates_history' not in self.config:
            self.config['dates_history'] = []

        self.config['dates_history'].append({
            'date_till': date_till,
            'date_of_pay': date_of_pay
        })

        # Сортировка по дате (новые первыми)
        self.config['dates_history'].sort(
            key=lambda x: x['date_till'],
            reverse=True
        )

        self.save()

    def get_last_date(self) -> Optional[str]:
        """Возвращает последнюю обработанную дату."""
        return self.config.get('last_date')

    def set_last_date(self, date: str):
        """Устанавливает последнюю обработанную дату."""
        self.config['last_date'] = date
        self.save()

    def get_earnings_percentage(self) -> float:
        """Возвращает процент заработка."""
        return self.config.get('settings', {}).get('earnings_percentage', EARNINGS_PERCENTAGE)
