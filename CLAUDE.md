# CLAUDE

Общие правила живут в глобальном `~/.claude/CLAUDE.md` и грузятся программой сами. Здесь только то, что касается этого проекта.

Read this before working in the project.

## Goal

Maintain the RaketaClean client-facing Telegram bot without confusing live code with historical notes. This bot is small, but it writes into shared business tables, so changes must be narrow and verified.

## Read Order

Infra map (canonical, token-light): /Users/evgenijpastusenko/Projects/agent1/docs/INFRA_MAP_LITE.yaml

1. `./AGENT_STATE.md`
2. recent entries in `./SESSION_LOG.md`
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

- `bot.py.map.md` — function map for `bot.py` (1718 lines); read the map first, then the needed part.

## Working Rules

- Treat `bot.py` as the primary runtime entrypoint unless the architecture is explicitly refactored.
- Verify database assumptions against `app/db.py` and migrations before changing stateful flows.
- If docs and code diverge, trust code and note the mismatch in `SESSION_LOG.md`.
- Treat `docs/spec.md` and `docs/dev_guide.md` as low-trust historical artifacts unless confirmed by code.
- Remember this bot touches shared RaketaClean tables; avoid schema or semantic changes without checking wider impact.
- Keep fixes narrow. Do not refactor `bot.py` broadly unless the task truly requires it.
- Record environment-sensitive changes clearly because the project depends on bot tokens and admin IDs.

## Deploy Rules

- After each deploy, verify the bot's logs.
