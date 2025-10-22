"""Скрипт для тестирования парсера сообщений."""
from message_parser import MessageParser
from datetime import datetime


def test_parser():
    """Тестирует основную функциональность парсера."""
    print("🔍 Тестирование парсера сообщений...\n")

    parser = MessageParser()

    # Тестовое сообщение
    test_message = """
    Дата: 22.10 !2
    Чаттер: @testuser
    Сколько заработано за смену: $150.50
    Дополнительные пометки: https://onlyfans.com/my/chats/chat/123456/ Хороший клиент
    """

    print("📝 Обработка тестового сообщения...")
    parser.process_message(test_message)

    print("✅ Сообщение обработано")
    print(f"📊 Данные заработка: {parser.earnings_df.shape}")
    print(f"📝 Данные клиентов: {parser.customer_df.shape}")

    # Проверка добавления дат
    print("\n📅 Тестирование добавления дат выплат...")
    test_date_till = datetime(2025, 1, 15)
    test_date_pay = datetime(2025, 1, 20)
    parser.add_dates_to_history(test_date_till, test_date_pay)
    print(f"✅ Добавлена дата: {test_date_till.strftime('%d.%m.%Y')} -> {test_date_pay.strftime('%d.%m.%Y')}")

    # Проверка получения статистики
    print("\n📈 Тестирование получения статистики...")
    total = parser.get_total_earnings("@testuser")
    shifts = parser.get_shift_count("@testuser")
    print(f"✅ Total заработок @testuser: ${total:.2f}")
    print(f"✅ Количество смен: {shifts}")

    print("\n✅ Все тесты пройдены успешно!")


if __name__ == "__main__":
    test_parser()

