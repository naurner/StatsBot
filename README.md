# StatsBot — shift reports and payouts for a chat team

Telegram bot built for a client: an agency that manages creator accounts and pays its chat operators ("chatters") a share of what they earn per shift. Before the bot, earnings were counted by hand from Telegram messages. Now each chatter posts a shift report in a chat, the bot parses it and keeps the books in Excel.

First version built in March 2025, rewritten in October 2025.

## What it does

- **Parses shift reports** written by people, not forms — date, chatter's @username, shift earnings, free-form notes. Regexes tolerate extra spaces, `$` signs, commas and a second shift on the same day (`!2`).
- **Keeps earnings in Excel** (pandas + openpyxl): one row per day, one column per chatter, so the owner can open the file and check any number.
- **Customer notes**: links to customer chats with comments are stored on a separate sheet; `/infp <link>` shows the whole history for that customer.
- **Payout periods**: `/setlast <cut-off date> <payout date>` records a payout; history is kept in `data/config.json`.
- **Per-chatter stats**: `/profile [@username]` — gross and net earnings (share is configurable, 15% by default) for the current and previous payout period and all time, shift count, average per shift.

## Example

A chatter sends:

```
Дата: 22.10
Чаттер: @anna
Сколько заработано за смену: $150.50
Дополнительные пометки: https://onlyfans.com/my/chats/chat/123456/ asked about a custom video
```

The bot replies "Сообщение обработано и данные сохранены", adds $150.50 to @anna's column for 22.10 and saves the note under that customer link.

## Stack

Python 3.11+ · python-telegram-bot 20 (async) · pandas · openpyxl · pytest

## Project layout

| File | Purpose |
|---|---|
| `bot.py` | entry point, registers commands and the message handler |
| `handlers.py` | `/profile`, `/infp`, `/setlast` and incoming reports |
| `message_parser.py` | report parsing, Excel storage, earnings calculations |
| `config.py` | paths, token loading, payout history (`ConfigManager`) |
| `migrate.py` | one-off migration from the v1 file layout to `data/` |
| `test_parser.py` | tests for parsing, stats and payout dates |

## Run

```bash
git clone https://github.com/naurner/StatsBot
cd StatsBot
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
export TELEGRAM_TOKEN=123456:your-bot-token       # or put it into telegram_token.txt
python bot.py
```

Data is written to `data/model.xlsx` and `data/config.json` (both are git-ignored).

## Tests

```bash
pip install pytest
pytest -v
```

Tests run against a temporary folder and never touch `data/`.

## History

- **v1 (March 2025)** — a single script running in Google Colab.
- **v2 (October 2025)** — split into modules, token moved out of the code, payout history moved to JSON, migration script for existing data.
