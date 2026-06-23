# Telegram Earnings Bot

Telegram bot for freelancers and small businesses to track income and expenses. Parses free-form messages, stores transactions, and exports reports to Excel.

## Features

- **Natural language input** — type "received 5000" or "spent 1200 on supplies"
- **Message parser** — extracts amount, category, and description automatically
- **SQLite storage** — lightweight, no server needed
- **Excel reports** — monthly breakdown with charts
- **Category tracking** — auto-tags transactions by keyword patterns
- **Balance summary** — running total on demand

## Stack

`Python` `aiogram` `SQLite` `openpyxl` `pytest`

## Quick Start

```bash
git clone https://github.com/naurner/StatsBot
cd StatsBot
pip install -r requirements.txt
```

Set your bot token in `config.py`:

```python
BOT_TOKEN = "your_token_here"
DB_PATH   = "stats.db"
```

Run migrations and start:

```bash
python migrate.py
python bot.py
```

## Tests

```bash
pytest test_parser.py -v
```

