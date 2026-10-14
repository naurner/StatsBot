"""Тесты парсера отчётов о сменах. Запуск: pytest -v"""
from datetime import datetime

import pytest

import config
import message_parser
from message_parser import MessageParser


REPORT = """
Дата: 22.10
Чаттер: @testuser
Сколько заработано за смену: $150.50
Дополнительные пометки: https://onlyfans.com/my/chats/chat/123456/ Хороший клиент
"""


@pytest.fixture
def parser(tmp_path, monkeypatch):
    """Парсер, который пишет Excel и конфиг во временную папку, а не в data/."""
    monkeypatch.setattr(message_parser, "EXCEL_PATH", tmp_path / "model.xlsx")
    monkeypatch.setattr(
        message_parser, "ConfigManager", lambda: config.ConfigManager(tmp_path / "config.json")
    )
    return MessageParser()


def test_report_adds_earning_and_shift(parser):
    parser.process_message(REPORT)

    assert parser.get_total_earnings("@testuser") == pytest.approx(150.50)
    assert parser.get_shift_count("@testuser") == 1


def test_report_saves_customer_note(parser):
    parser.process_message(REPORT)

    notes = parser.get_customer_notes("https://onlyfans.com/my/chats/chat/123456/")
    assert not notes.empty
    assert "Хороший клиент" in notes.iloc[0, 1]


def test_unknown_user_has_no_stats(parser):
    assert parser.get_total_earnings("@nobody") == 0.0
    assert parser.get_shift_count("@nobody") == 0


def test_payout_dates_history(parser):
    parser.add_dates_to_history(datetime(2025, 1, 15), datetime(2025, 1, 20))
    parser.add_dates_to_history(datetime(2025, 1, 31), datetime(2025, 2, 5))

    assert parser.get_nearest_payout_date() == datetime(2025, 1, 31)
    assert parser.get_previous_payout_date() == datetime(2025, 1, 15)
    assert parser.last_date == datetime(2025, 2, 5)


def test_save_and_reload(parser, tmp_path):
    parser.process_message(REPORT)
    parser.save_to_excel()

    reloaded = MessageParser()
    assert reloaded.get_total_earnings("@testuser") == pytest.approx(150.50)
