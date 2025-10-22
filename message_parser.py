"""Парсер сообщений и работа с данными."""
import logging
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from config import (
    EXCEL_PATH, EARNINGS_SHEET, CUSTOMER_INFO_SHEET,
    ConfigManager
)


logger = logging.getLogger(__name__)


class MessageParser:
    """Класс для парсинга сообщений и управления данными о заработке."""

    def __init__(self):
        self.config_manager = ConfigManager()
        self.earnings_df = self._load_excel_data(EARNINGS_SHEET)
        self.customer_df = self._load_excel_data(CUSTOMER_INFO_SHEET)

        # Подготовка customer_df с индексом Date
        if 'Date' in self.customer_df.columns:
            self.customer_df['Date'] = pd.to_datetime(self.customer_df['Date'], errors='coerce')
            self.customer_df.set_index('Date', inplace=True)
        else:
            self.customer_df = pd.DataFrame()

        self.dates_history = self._load_dates_history()
        self._update_dates_from_history()

        # Обеспечить столбец Date в earnings_df
        if 'Date' not in self.earnings_df.columns:
            self.earnings_df['Date'] = pd.to_datetime(pd.Series(dtype='datetime64[ns]'))
        else:
            self.earnings_df['Date'] = pd.to_datetime(self.earnings_df['Date'], errors='coerce')

        logger.info("Парсер инициализирован успешно.")

    def _load_excel_data(self, sheet_name: str) -> pd.DataFrame:
        """Загружает данные из Excel файла."""
        try:
            if not EXCEL_PATH.exists():
                logger.warning(f"Файл {EXCEL_PATH} не найден. Создан пустой DataFrame.")
                return pd.DataFrame()

            try:
                df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                logger.info(f"Данные загружены из листа '{sheet_name}'.")
                return df
            except ValueError:
                logger.info(f"Лист '{sheet_name}' отсутствует. Создан пустой DataFrame.")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из листа '{sheet_name}': {e}")
            return pd.DataFrame()

    def _load_dates_history(self) -> List[Tuple[datetime, datetime]]:
        """Загружает историю дат выплат из конфигурации."""
        try:
            dates_list = self.config_manager.get_dates_history()
            parsed = []

            for item in dates_list:
                try:
                    dt = datetime.fromisoformat(item['date_till'])
                    dp = datetime.fromisoformat(item['date_of_pay'])
                    parsed.append((dt, dp))
                except Exception as e:
                    logger.warning(f"Ошибка при парсинге дат из конфигурации: {e}")
                    continue

            return parsed
        except Exception as e:
            logger.error(f"Ошибка при загрузке истории дат: {e}")
            return []

    def _update_dates_from_history(self):
        """Обновляет текущие даты из истории."""
        if self.dates_history:
            self.date_till, self.last_date = self.dates_history[0]
        else:
            self.date_till = datetime.min
            self.last_date = datetime.min

    def add_dates_to_history(self, date_till: datetime, date_of_pay: datetime):
        """Добавляет новые даты в историю."""
        self.config_manager.add_dates_to_history(
            date_till.isoformat(),
            date_of_pay.isoformat()
        )
        self.dates_history = self._load_dates_history()
        self._update_dates_from_history()

    def get_nearest_payout_date(self) -> datetime:
        """Возвращает ближайшую дату выплаты."""
        return self.date_till

    def get_previous_payout_date(self) -> datetime:
        """Возвращает предыдущую дату выплаты."""
        if len(self.dates_history) > 1:
            return self.dates_history[1][0]
        if self.date_till != datetime.min:
            return self.date_till - timedelta(days=15)
        return datetime.min

    def _find_nearest_date_with_earnings(self, username: str, target_date: Optional[datetime]) -> Optional[datetime]:
        """Находит ближайшую дату с заработком для пользователя."""
        if username not in self.earnings_df.columns or target_date is None:
            return None

        dates_series = self.earnings_df.loc[self.earnings_df[username].notna(), 'Date'].dropna()
        if dates_series.empty:
            return None

        diffs = (dates_series - target_date).abs()
        nearest_idx = diffs.idxmin()
        return dates_series.loc[nearest_idx]

    def _ensure_earning_column(self, username: str):
        """Создает колонку для пользователя, если её нет."""
        if username not in self.earnings_df.columns:
            self.earnings_df[username] = pd.NA

    def _upsert_earning(self, date: datetime, username: str, amount: float, hour_match: Optional[int] = None):
        """Добавляет или обновляет запись о заработке."""
        self._ensure_earning_column(username)

        if hour_match is None:
            mask = self.earnings_df['Date'] == date
        else:
            mask = (self.earnings_df['Date'] == date) & (self.earnings_df['Date'].dt.hour == hour_match)

        existing = self.earnings_df.loc[mask]

        if not existing.empty:
            idx = existing.index[0]
            current = pd.to_numeric(self.earnings_df.at[idx, username], errors='coerce')
            if pd.isna(current):
                self.earnings_df.at[idx, username] = amount
            else:
                self.earnings_df.at[idx, username] = current + amount
        else:
            new_row = {col: pd.NA for col in self.earnings_df.columns}
            new_row['Date'] = date
            new_row[username] = amount
            self.earnings_df = pd.concat([self.earnings_df, pd.DataFrame([new_row])], ignore_index=True)

    @staticmethod
    def _normalize_message(text: str) -> str:
        """Нормализует текст сообщения."""
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{2,}', '\n', text).strip()
        return re.sub(r'[ \t]+', ' ', text).strip()

    @staticmethod
    def _parse_short_date(date_str: str) -> Optional[datetime]:
        """Парсит короткую дату типа '1.2' или '01.02'."""
        date_str = date_str.replace(',', '.')
        parts = date_str.split('.')
        if len(parts) != 2:
            return None

        day, month = parts
        day = day.zfill(2)
        month = month.zfill(2)
        year = datetime.now().year

        try:
            return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
        except Exception:
            return None

    def process_message(self, text: str):
        """Обрабатывает текст сообщения и извлекает данные о заработке."""
        if not text:
            return

        text = self._normalize_message(text)

        # Регулярные выражения для парсинга
        date_pattern = r'\b(\d{1,2}\.\d{1,2})\s*(!2)?\b'
        chatter_pattern = r"Чаттер:\s*(@\w+)"
        earnings_pattern = r"Сколько\s*заработано\s*за\s*смену:\s*\$?([\d,]+(?:\.\d+)?)"
        notes_pattern = r"Дополнительные\s*пометки:\s*(.*)"
        additional_earnings_pattern = r"(@\w+)\s*\$([\d,]+(?:\.\d+)?)"

        date_match = re.search(date_pattern, text)
        chatter_match = re.search(chatter_pattern, text)
        earnings_match = re.search(earnings_pattern, text)
        notes_match = re.search(notes_pattern, text, re.DOTALL)
        additional_matches = re.findall(additional_earnings_pattern, text)

        date = None
        is_second_entry = False

        if date_match:
            parsed = self._parse_short_date(date_match.group(1))
            is_second_entry = bool(date_match.group(2))
            if parsed:
                date = parsed
                if is_second_entry:
                    date = date.replace(hour=12, minute=0, second=0)

        chatter = chatter_match.group(1) if chatter_match else None
        earnings_raw = earnings_match.group(1) if earnings_match else None
        notes = notes_match.group(1).strip() if notes_match else None

        # Обработка основного заработка
        if earnings_raw and date and chatter:
            earnings_val = float(earnings_raw.replace(',', '.'))
            self._upsert_earning(date, chatter, earnings_val, hour_match=(12 if is_second_entry else None))

            if date > self.last_date:
                self.last_date = date
                self.config_manager.set_last_date(date.isoformat())

        # Обработка дополнительных заработков
        for uname, val in additional_matches:
            try:
                v = float(val.replace(',', '.'))
            except Exception:
                logger.warning(f"Не удалось преобразовать сумму {val} для {uname}")
                continue

            nearest = self._find_nearest_date_with_earnings(uname, date) if date is not None else None
            if nearest is not None:
                self._upsert_earning(nearest, uname, v)
            elif date is not None:
                self._upsert_earning(date, uname, v)

        # Обработка заметок (OnlyFans ссылки)
        if notes and date:
            chat_links_notes = re.findall(
                r'(https://onlyfans\.com/my/chats/chat/\d+/)\s*(.*?)(?=\s*https://|$)',
                notes,
                re.DOTALL
            )

            for link, note in chat_links_notes:
                note = note.strip()
                if self.customer_df.empty:
                    self.customer_df = pd.DataFrame(index=pd.DatetimeIndex([], name='Date'))

                if link not in self.customer_df.columns:
                    self.customer_df[link] = pd.NA

                self.customer_df.at[date, link] = note

    def save_to_excel(self):
        """Сохраняет данные в Excel файл."""
        try:
            with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
                self.earnings_df.to_excel(writer, sheet_name=EARNINGS_SHEET, index=False)

                if not self.customer_df.empty:
                    self.customer_df.reset_index().to_excel(
                        writer, sheet_name=CUSTOMER_INFO_SHEET, index=False
                    )
                else:
                    pd.DataFrame().to_excel(writer, sheet_name=CUSTOMER_INFO_SHEET, index=False)

            logger.info(f"Данные успешно сохранены в {EXCEL_PATH}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в Excel: {e}")

    def get_earnings_sum(self, username: str, start_date: datetime, end_date: Optional[datetime] = None) -> float:
        """Возвращает сумму заработка пользователя за период."""
        if username not in self.earnings_df.columns:
            return 0.0

        series = pd.to_numeric(self.earnings_df[username], errors='coerce')
        mask = self.earnings_df['Date'] > start_date if start_date is not None else pd.Series([True] * len(self.earnings_df))

        if end_date:
            mask &= (self.earnings_df['Date'] <= end_date)

        return float(series.loc[mask].sum())

    def get_total_earnings(self, username: str) -> float:
        """Возвращает общий заработок пользователя."""
        if username not in self.earnings_df.columns:
            return 0.0
        return float(pd.to_numeric(self.earnings_df[username], errors='coerce').sum())

    def get_shift_count(self, username: str) -> int:
        """Возвращает количество смен пользователя."""
        if username not in self.earnings_df.columns:
            return 0
        return int(self.earnings_df[username].count())

    def get_customer_notes(self, link: str) -> pd.DataFrame:
        """Возвращает заметки по ссылке на клиента."""
        if link in self.customer_df.columns:
            return self.customer_df[[link]].dropna().reset_index()
        return pd.DataFrame()
