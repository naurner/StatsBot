import logging
import re
import pandas as pd
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import nest_asyncio
import os
import asyncio

nest_asyncio.apply()


TOKEN = '7896122656:AAF6oVFW0fLLIOFP0vjskFJpP75R0HdHp_k'
LAST_DATE_FILE = 'last_date.txt'
DATES_HISTORY_FILE = 'dates_history.txt'
EXCEL_PATH = "Nicole_earn_data.xlsx"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class MessageParser:
    def __init__(self):
        self.earnings_df = self.load_excel_data("заработок")
        self.customer_df = self.load_excel_data("инфо о покупателе")
        self.dates_history = self.load_dates_history()
        self.update_dates_from_history()

        if 'Date' not in self.earnings_df.columns:
            self.earnings_df['Date'] = None

        if 'Date' in self.earnings_df.columns:
            self.earnings_df['Date'] = pd.to_datetime(self.earnings_df['Date'], errors='coerce')
            logger.info(f"Данные после преобразования дат:\n{self.earnings_df}")

    def load_excel_data(self, sheet_name):
        try:
            if os.path.exists(EXCEL_PATH):
                df = pd.read_excel(EXCEL_PATH, sheet_name=sheet_name)
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                logger.info(f"Данные загружены из листа {sheet_name}:\n{df}")
                return df
            else:
                logger.warning(f"Файл {EXCEL_PATH} не найден. Создан новый DataFrame.")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из листа {sheet_name}: {e}")
            return pd.DataFrame()

    def load_dates_history(self):
        try:
            if not os.path.exists(DATES_HISTORY_FILE):
                with open(DATES_HISTORY_FILE, 'w') as f:
                    pass
                return []

            with open(DATES_HISTORY_FILE, 'r') as f:
                lines = f.readlines()
                dates = [line.strip().split() for line in lines if line.strip()]
                dates = sorted(
                    [(datetime.strptime(dt, '%Y-%m-%d'), datetime.strptime(dp, '%Y-%m-%d')) for dt, dp in dates],
                    key=lambda x: x[0],
                    reverse=True
                )
                return dates
        except Exception as e:
            logger.error(f"Ошибка при загрузке истории дат: {e}")
            return []

    def update_dates_from_history(self):
        if self.dates_history:
            self.date_till, self.last_date = self.dates_history[0]
        else:
            self.date_till = datetime.min
            self.last_date = datetime.min

    def save_dates_history(self):
        try:
            with open(DATES_HISTORY_FILE, 'w') as f:
                for dt, dp in self.dates_history:
                    f.write(f"{dt.strftime('%Y-%m-%d')} {dp.strftime('%Y-%m-%d')}\n")
        except Exception as e:
            logger.error(f"Ошибка при сохранении истории дат: {e}")

    def add_dates_to_history(self, date_till, date_of_pay):
        self.dates_history.append((date_till, date_of_pay))
        self.dates_history.sort(key=lambda x: x[0], reverse=True)
        self.save_dates_history()
        self.update_dates_from_history()

    def get_nearest_payout_date(self):
        return self.date_till

    def get_previous_payout_date(self):
        if len(self.dates_history) > 1:
            return self.dates_history[1][0]
        elif self.date_till != datetime.min:
            return self.date_till - timedelta(days=15)
        else:
            return datetime.min

    def find_nearest_date_with_earnings(self, username, target_date):
        if username not in self.earnings_df.columns:
            return None

        earnings_dates = self.earnings_df[self.earnings_df[username].notna()]['Date']

        if earnings_dates.empty:
            return None

        nearest_date = min(earnings_dates, key=lambda x: abs(x - target_date))
        return nearest_date

    def normalize_message(self, text):
        text = re.sub(r'\n+', '\n', text).strip()
        return re.sub(r'\s+', ' ', text).strip()

    def process_message(self, text):
        if not text:
            return

        text = self.normalize_message(text)

        date_pattern = r'\b(\d{1,2}\.\d{1,2})\s*(!2)?\b'
        chatter_pattern = r"Чаттер:\s*(@\w+)"
        earnings_pattern = r"Сколько\s*заработано\s*за\s*смену:\s*\$?([\d,]+(?:\.\d+)?)"
        notes_pattern = r"Дополнительные\s*пометки:\s*(.*)"
        additional_earnings_pattern = r"(@\w+)\s*\$([\d,]+(?:\.\d+)?)"

        date_match = re.search(date_pattern, text)
        chatter_match = re.search(chatter_pattern, text)
        earnings_match = re.search(earnings_pattern, text)
        notes_match = re.search(notes_pattern, text, re.DOTALL)
        additional_earnings_matches = re.findall(additional_earnings_pattern, text)

        date = date_match.group(1) if date_match else None
        is_second_entry = date_match.group(2) == "!2" if date_match else False

        if date:
            if len(date.split('.')[0]) == 1:
                date = f"0{date}"
            if len(date.split('.')[1]) == 1:
                date = f"{date.split('.')[0]}.0{date.split('.')[1]}"

            date = date.replace(",", ".")
            current_year = datetime.now().year
            date_with_year = f"{date}.{current_year}"
            try:
                date = pd.to_datetime(date_with_year, format='%d.%m.%Y')
                if is_second_entry:
                    date = date.replace(hour=12, minute=0)
            except ValueError:
                logger.warning(f"Не удалось преобразовать дату {date_with_year} в datetime.")
                date = None

        chatter = chatter_match.group(1) if chatter_match else None
        earnings = earnings_match.group(1) if earnings_match else None
        notes = notes_match.group(1).strip() if notes_match else None

        if earnings and date and chatter:
            earnings = earnings.replace(",", ".")
            earnings = pd.to_numeric(earnings, errors='coerce')
            if pd.isna(earnings):
                logger.warning(f"Не удалось преобразовать заработок в число: {earnings_match.group(1)}")
                return

            if chatter not in self.earnings_df.columns:
                self.earnings_df[chatter] = None

            existing_row = self.earnings_df[self.earnings_df['Date'] == date]

            if not existing_row.empty:
                idx = existing_row.index[0]
                self.earnings_df.at[idx, chatter] = earnings
            else:
                new_row = {'Date': date, chatter: earnings}
                self.earnings_df = pd.concat([self.earnings_df, pd.DataFrame([new_row])], ignore_index=True)

            if isinstance(date, datetime) and date > self.last_date:
                self.last_date = date
                self.save_date_to_file(self.last_date, LAST_DATE_FILE)

        for additional_chatter, additional_earning in additional_earnings_matches:
            additional_earning = additional_earning.replace(",", ".")
            additional_earning = pd.to_numeric(additional_earning, errors='coerce')
            if pd.isna(additional_earning):
                logger.warning(f"Не удалось преобразовать дополнительный заработок в число: {additional_earning}")
                continue

            if additional_chatter not in self.earnings_df.columns:
                self.earnings_df[additional_chatter] = None

            nearest_date = self.find_nearest_date_with_earnings(additional_chatter, date)

            if nearest_date is not None:
                existing_row = self.earnings_df[self.earnings_df['Date'] == nearest_date]
                if not existing_row.empty:
                    idx = existing_row.index[0]
                    if pd.isna(self.earnings_df.at[idx, additional_chatter]):
                        self.earnings_df.at[idx, additional_chatter] = additional_earning
                    else:
                        self.earnings_df.at[idx, additional_chatter] += additional_earning
            else:
                new_row = {'Date': date, additional_chatter: additional_earning}
                self.earnings_df = pd.concat([self.earnings_df, pd.DataFrame([new_row])], ignore_index=True)

        if is_second_entry and earnings and date and chatter:
            if chatter not in self.earnings_df.columns:
                self.earnings_df[chatter] = None

            existing_second_entry = self.earnings_df[
                (self.earnings_df['Date'] == date) &
                (self.earnings_df['Date'].dt.hour == 12)
            ]

            if not existing_second_entry.empty:
                idx = existing_second_entry.index[0]
                if pd.isna(self.earnings_df.at[idx, chatter]):
                    self.earnings_df.at[idx, chatter] = earnings
                else:
                    self.earnings_df.at[idx, chatter] += earnings
            else:
                new_row = {'Date': date.replace(hour=12, minute=0), chatter: earnings}
                self.earnings_df = pd.concat([self.earnings_df, pd.DataFrame([new_row])], ignore_index=True)

        if notes and date:
            chat_links_notes = re.findall(r'(https://onlyfans\.com/my/chats/chat/\d+/)\s*(.*?)(?=\s*https://|$)', notes, re.DOTALL)
            for link, note in chat_links_notes:
                note = note.strip()
                if link not in self.customer_df.columns:
                    self.customer_df[link] = None

                if date in self.customer_df.index:
                    self.customer_df.at[date, link] = note
                else:
                    new_row = {link: note}
                    self.customer_df.loc[date] = new_row

    def save_to_excel(self):
        try:
            with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
                self.earnings_df.to_excel(writer, sheet_name="заработок", index=False)
                self.customer_df.reset_index().rename(columns={'index': 'Date'}).to_excel(writer, sheet_name="инфо о покупателе", index=False)
            logger.info("Данные успешно сохранены в Excel.")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в Excel: {e}")

    def get_earnings_sum(self, username, start_date, end_date=None):
        if username not in self.earnings_df.columns:
            return 0.0

        earnings = pd.to_numeric(self.earnings_df[username], errors='coerce')
        mask = (self.earnings_df['Date'] > start_date)
        if end_date:
            mask &= (self.earnings_df['Date'] <= end_date)

        return self.earnings_df.loc[mask, username].sum()

    def get_total_earnings(self, username):
        if username not in self.earnings_df.columns:
            return 0.0
        return pd.to_numeric(self.earnings_df[username], errors='coerce').sum()

parser = MessageParser()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.startswith('/'):
        return

    logger.info(f"Получено сообщение: {update.message.text}")

    if update.message.photo or update.message.document or update.message.audio or update.message.video:
        text = update.message.caption
        if not text:
            await update.message.reply_text("Сообщение с файлом не содержит текста для обработки.")
            return
    else:
        text = update.message.text

    if text == '!mkdir -p "/content/drive/MyDrive/TeleGram Sudo"':
        os.makedirs("/content/drive/MyDrive/TeleGram Sudo", exist_ok=True)
        await update.message.reply_text("Директория создана или уже существует.")
    elif text:
        parser.process_message(text)
        parser.save_to_excel()
        await update.message.reply_text("Сообщение обработано и данные сохранены.")
    else:
        await update.message.reply_text("Сообщение не содержит текста для обработки.")

async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        username = context.args[0]
        if not username.startswith('@'):
            username = f"@{username}"
    else:
        username = update.message.from_user.username
        if not username:
            await update.message.reply_text("У вас не установлен юзернейм в Telegram.")
            return
        username = f"@{username}"

    nearest_payout_date = parser.get_nearest_payout_date()
    previous_payout_date = parser.get_previous_payout_date()

    try:
        current_start_date = nearest_payout_date
        current_end_date = datetime.now()
        current_earnings_sum = parser.get_earnings_sum(username, current_start_date, current_end_date)
        current_net_earnings = current_earnings_sum * 0.15 if current_earnings_sum is not None else 0

        previous_start_date = previous_payout_date
        previous_end_date = nearest_payout_date
        previous_earnings_sum = parser.get_earnings_sum(username, previous_start_date, previous_end_date)
        previous_net_earnings = previous_earnings_sum * 0.15 if previous_earnings_sum is not None else 0

        previous_payout_date_actual = (
            parser.dates_history[1][1].strftime('%d.%m.%Y')
            if len(parser.dates_history) > 1
            else "Нет данных"
        )

        total_earnings = parser.get_total_earnings(username)
        total_net_earnings = total_earnings * 0.15
        shift_count = parser.earnings_df[username].count() if username in parser.earnings_df.columns else 0
        average_earnings = total_earnings / shift_count if shift_count > 0 else 0

        message = (
            f"Статистика {username}\n"
            f"Total чаттера с последней расчетной даты составляет: ${current_earnings_sum:.2f}\n"
            f"Чистый заработок чаттера с последней расчетной даты составляет: ${current_net_earnings:.2f}\n"
            f"Дата последней выплаты: {parser.last_date.strftime('%d.%m.%Y')}\n"
            f"Расчетная дата последней выплаты: {nearest_payout_date.strftime('%d.%m.%Y')}\n"
            f"--------------------------------------------------------------------------------------------\n"
            f"Инфо о прошлой выплате\n"
            f"Total чаттера с прошлой расчетной даты составляет: ${previous_earnings_sum:.2f}\n"
            f"Чистый заработок чаттера с прошлой расчетной даты составляет: ${previous_net_earnings:.2f}\n"
            f"Дата предпоследней выплаты: {previous_payout_date_actual}\n"
            f"Расчетная дата предпоследней выплаты: {previous_payout_date.strftime('%d.%m.%Y')}\n"
            f"------------------------------------------------------------------------------------------\n"
            f"Общая статистика\n"
            f"Total чаттера за все время составляет: ${total_earnings:.2f}\n"
            f"Чистый заработок чаттера за все время составляет: ${total_net_earnings:.2f}\n"
            f"Средний заработок чаттера за смену: ${average_earnings:.2f}\n"
            f"Кол-во смен чаттера: {shift_count}"
        )

        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /profile: {e}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

async def infp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажите ссылку после команды /infp.")
        return

    link = context.args[0]
    if link in parser.customer_df.columns:
        notes_with_dates = parser.customer_df[['Date', link]].dropna()
        if not notes_with_dates.empty:
            message = "Заметки по этой ссылке:\n"
            for date, note in notes_with_dates.itertuples(index=False):
                message += f"{date.strftime('%d.%m.%Y')}: {note}\n"
        else:
            message = "Заметок нет."
    else:
        message = "Заметок нет."
    await update.message.reply_text(message)

async def setlast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 2:
        await update.message.reply_text("Используйте команду в формате: /setlast {date_till} {date_of_pay}")
        return

    try:
        date_till = datetime.strptime(context.args[0], '%d.%m.%Y')
        date_of_pay = datetime.strptime(context.args[1], '%d.%m.%Y')

        parser.add_dates_to_history(date_till, date_of_pay)

        await update.message.reply_text(
            f"Даты обновлены:\n"
            f"Расчетная дата: {date_till.strftime('%d.%m.%Y')}\n"
            f"Дата выплаты: {date_of_pay.strftime('%d.%m.%Y')}"
        )
    except ValueError as e:
        await update.message.reply_text(f"Ошибка в формате даты. Используйте формат ДД.ММ.ГГГГ: {e}")

async def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("profile", profile_command))
    application.add_handler(CommandHandler("infp", infp_command))
    application.add_handler(CommandHandler("setlast", setlast_command))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_message))
    logger.info("Бот запущен...")
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())