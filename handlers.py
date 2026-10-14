"""Обработчики команд и сообщений Telegram-бота."""
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from message_parser import MessageParser


logger = logging.getLogger(__name__)

SEPARATOR = "-" * 40


class BotHandlers:
    """Связывает команды бота с MessageParser."""

    def __init__(self, parser: MessageParser):
        self.parser = parser

    @staticmethod
    def _resolve_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Берёт юзернейм из аргумента команды или из профиля отправителя."""
        if context.args:
            username = context.args[0]
            return username if username.startswith("@") else f"@{username}"
        if update.message.from_user.username:
            return f"@{update.message.from_user.username}"
        return None

    @staticmethod
    def _fmt(date: datetime) -> str:
        return "Нет данных" if date == datetime.min else date.strftime("%d.%m.%Y")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Принимает отчёт о смене (текст или подпись к файлу) и сохраняет его."""
        message = update.message
        text = message.caption if (message.photo or message.document or message.audio or message.video) else message.text

        if not text:
            await message.reply_text("Сообщение не содержит текста для обработки.")
            return

        self.parser.process_message(text)
        self.parser.save_to_excel()
        await message.reply_text("Сообщение обработано и данные сохранены.")

    async def profile_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/profile [@username] — статистика чаттера за текущий и прошлый расчётный период."""
        username = self._resolve_username(update, context)
        if not username:
            await update.message.reply_text("У вас не установлен юзернейм в Telegram.")
            return

        p = self.parser
        percentage = p.config_manager.get_earnings_percentage()
        nearest = p.get_nearest_payout_date()
        previous = p.get_previous_payout_date()

        try:
            current_sum = p.get_earnings_sum(username, nearest, datetime.now())
            previous_sum = p.get_earnings_sum(username, previous, nearest)
            total = p.get_total_earnings(username)
            shifts = p.get_shift_count(username)
            average = total / shifts if shifts else 0
            previous_pay = p.dates_history[1][1] if len(p.dates_history) > 1 else datetime.min

            text = (
                f"Статистика {username}\n"
                f"Total с последней расчётной даты: ${current_sum:.2f}\n"
                f"Чистый заработок с последней расчётной даты: ${current_sum * percentage:.2f}\n"
                f"Дата последней выплаты: {self._fmt(p.last_date)}\n"
                f"Расчётная дата последней выплаты: {self._fmt(nearest)}\n"
                f"{SEPARATOR}\n"
                f"Прошлая выплата\n"
                f"Total за прошлый период: ${previous_sum:.2f}\n"
                f"Чистый заработок за прошлый период: ${previous_sum * percentage:.2f}\n"
                f"Дата предпоследней выплаты: {self._fmt(previous_pay)}\n"
                f"Расчётная дата предпоследней выплаты: {self._fmt(previous)}\n"
                f"{SEPARATOR}\n"
                f"За всё время\n"
                f"Total: ${total:.2f}\n"
                f"Чистый заработок: ${total * percentage:.2f}\n"
                f"Средний заработок за смену: ${average:.2f}\n"
                f"Количество смен: {shifts}"
            )
            await update.message.reply_text(text)
        except Exception as e:
            logger.exception("Ошибка при обработке /profile")
            await update.message.reply_text(f"Произошла ошибка: {e}")

    async def infp_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/infp <ссылка> — все заметки чаттеров по клиенту."""
        if not context.args:
            await update.message.reply_text("Укажите ссылку после команды /infp.")
            return

        notes = self.parser.get_customer_notes(context.args[0])
        if notes.empty:
            await update.message.reply_text("Заметок нет.")
            return

        lines = ["Заметки по этой ссылке:"]
        for date, note in notes.itertuples(index=False):
            lines.append(f"{date.strftime('%d.%m.%Y')}: {note}")
        await update.message.reply_text("\n".join(lines))

    async def setlast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/setlast ДД.ММ.ГГГГ ДД.ММ.ГГГГ — расчётная дата и дата выплаты."""
        if len(context.args) != 2:
            await update.message.reply_text("Формат: /setlast <расчётная дата> <дата выплаты>, например /setlast 15.10.2025 20.10.2025")
            return

        try:
            date_till = datetime.strptime(context.args[0], "%d.%m.%Y")
            date_of_pay = datetime.strptime(context.args[1], "%d.%m.%Y")
        except ValueError:
            await update.message.reply_text("Неверный формат даты. Используйте ДД.ММ.ГГГГ.")
            return

        self.parser.add_dates_to_history(date_till, date_of_pay)
        await update.message.reply_text(
            f"Даты обновлены:\nРасчётная дата: {self._fmt(date_till)}\nДата выплаты: {self._fmt(date_of_pay)}"
        )
