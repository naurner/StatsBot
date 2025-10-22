"""Главный файл для запуска Telegram бота."""
import logging
import asyncio
import nest_asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

from config import TELEGRAM_TOKEN
from message_parser import MessageParser
from handlers import BotHandlers


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Применяем nest_asyncio для совместимости с некоторыми средами
nest_asyncio.apply()


async def main():
    """Главная функция для запуска бота."""
    if not TELEGRAM_TOKEN:
        logger.error(
            "TELEGRAM_TOKEN не задан. "
            "Установите переменную окружения или создайте файл telegram_token.txt"
        )
        return

    # Инициализация
    parser = MessageParser()
    handlers = BotHandlers(parser)

    # Создание приложения
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Регистрация команд
    application.add_handler(CommandHandler("profile", handlers.profile_command))
    application.add_handler(CommandHandler("infp", handlers.infp_command))
    application.add_handler(CommandHandler("setlast", handlers.setlast_command))

    # Обработчик для текстовых сообщений и медиа с подписями
    application.add_handler(
        MessageHandler(
            (filters.TEXT | filters.PHOTO | filters.Document.ALL |
             filters.AUDIO | filters.VIDEO) & ~filters.COMMAND,
            handlers.handle_message
        )
    )

    logger.info("🚀 Бот запущен и готов к работе...")

    # Запуск бота
    await application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    asyncio.run(main())

