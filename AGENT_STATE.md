# AGENT_STATE

project: telegram-bot-client
last_updated: 2026-04-13
updated_by: codex
status: active
confidence: high

## Purpose

`telegram-bot-client` is a small client-facing Telegram bot for RaketaClean. It is a companion project to the broader RaketaClean bot ecosystem and handles onboarding, phone capture, signup bonus logic, simple client interactions, and admin notifications.

## Current State

The live runtime is concentrated in `bot.py` and runs by long polling. The bot asks for a phone number, links or creates a client in the shared database, grants a one-time signup bonus, allows the user to ask a question, request an order, send media for evaluation, view bonus balance, and read static price/work-schedule info. It writes into shared business tables such as `clients`, `bonus_transactions`, `orders`, and `leads`, so changes here can affect the wider RaketaClean stack. The DB pool helper lives in `app/db.py`, and the only local SQL migration currently present is `app/migrations/0003_client_bot.sql`. `bot.py` now also includes a Telegram API IP fallback resolver modeled on `tgbot-v1`, with env overrides plus a default probe pool.

## Active Focus

Deploy and verify the Telegram API IP fallback in production so long polling survives a bad `api.telegram.org` DNS answer from the host.

## Known Risks

- `bot.py` is the runtime truth and the surrounding docs are partly stale or contradictory.
- `docs/spec.md` is effectively empty and `docs/dev_guide.md` describes an older or different project shape.
- `telegram_bot_full_spec.md` and amoCRM-related docs contain useful business context but should not be treated as final implementation truth.
- The bot modifies shared RaketaClean tables, so schema or behavioral changes should be checked against the wider ecosystem, especially `tgbot-v1`.
- The fallback uses a baked-in Telegram IP pool unless env overrides are set; if Telegram rotates reachable ingress IPs, the pool may need refresh.

## Source Of Truth

- `bot.py`
- `app/db.py`
- `app/migrations/0003_client_bot.sql`
- `docs/NEW_BOT_LOGIC.md`
- `telegram_bot_full_spec.md`
- `docs/TELEGRAM_BOT_INTEGRATION.md`
