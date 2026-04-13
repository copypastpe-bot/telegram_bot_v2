# CLAUDE

Read this before working in the project.

## Goal

Maintain the RaketaClean client-facing Telegram bot without confusing live code with historical notes. This bot is small, but it writes into shared business tables, so changes must be narrow and verified.

## Read Order

1. `AGENT_STATE.md`
2. recent entries in `SESSION_LOG.md`
3. `bot.py`
4. `app/db.py`
5. `app/migrations/0003_client_bot.sql`
6. `docs/NEW_BOT_LOGIC.md`
7. `telegram_bot_full_spec.md`
8. `docs/TELEGRAM_BOT_INTEGRATION.md`

## Key Sources

- `bot.py`
- `app/db.py`
- `app/migrations/0003_client_bot.sql`
- `docs/NEW_BOT_LOGIC.md`
- `telegram_bot_full_spec.md`
- `docs/TELEGRAM_BOT_INTEGRATION.md`

## Working Rules

- Treat `bot.py` as the primary runtime entrypoint unless the architecture is explicitly refactored.
- Verify database assumptions against `app/db.py` and migrations before changing stateful flows.
- If docs and code diverge, trust code and note the mismatch in `SESSION_LOG.md`.
- Treat `docs/spec.md` and `docs/dev_guide.md` as low-trust historical artifacts unless confirmed by code.
- Remember this bot touches shared RaketaClean tables; avoid schema or semantic changes without checking wider impact.
- Keep fixes narrow. Do not refactor `bot.py` broadly unless the task truly requires it.
- Record environment-sensitive changes clearly because the project depends on bot tokens and admin IDs.

## End Of Session Requirements

Before ending the session:
1. rewrite `AGENT_STATE.md` to reflect current state;
2. append one new entry to `SESSION_LOG.md`;
3. keep both files short, factual, and agent-readable.

## Current Focus

Prepare the project for the next repair session by anchoring work to the live runtime flow: phone capture, signup bonus, questions/orders/media forwarding, and admin notifications.
