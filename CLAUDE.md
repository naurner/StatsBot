# CLAUDE.md

Telegram bot for an agency: chatters post free-text shift reports, the bot parses them and keeps earnings, customer notes and payout periods in Excel.

## Stack
Python 3.11+ · python-telegram-bot 20 (async) · pandas · openpyxl · pytest

## Layout
- `bot.py` — entry point, registers handlers
- `handlers.py` — `/profile`, `/infp`, `/setlast`, incoming reports
- `message_parser.py` — report parsing, Excel storage, earnings math
- `config.py` — paths, token loading, payout history (`ConfigManager`)
- `migrate.py` — one-off v1 → `data/` migration, do not change
- `test_parser.py` — tests for parsing and storage

## Commands
- Install: `pip install -r requirements.txt`
- Run: `TELEGRAM_TOKEN=... python bot.py`
- Test: `pytest -q`

## Rules
- Never commit secrets or client data: token comes only from `TELEGRAM_TOKEN` env or `telegram_token.txt` (gitignored). No `.xlsx`, `.csv` or `.json` from `data/` in commits.
- Reports are written by people, not forms. Any parser change must keep tolerating extra spaces, `$`, commas and second shifts (`!2`). Add a test case for every new format you support.
- Excel layout is read by the owner by hand: one row per day, one column per chatter. Do not rename sheets or columns.
- Money: keep amounts as floats rounded to 2 decimals; the chatter share (default 15%) lives in config, never hardcode it.
- Bot replies stay in Russian.

## Workflow
1. For anything bigger than a one-line fix: plan first, list the files you'll touch, wait for approval.
2. Make the change, add or update tests.
3. Run `pytest -q` — task is not done until tests pass.
4. Report what changed and how it was verified. If something is unclear, research first, then ask with options and a recommendation.
