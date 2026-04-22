# CLAUDE

Read this before working in the project.

## Goal

Maintain the RaketaClean client-facing Telegram bot without confusing live code with historical notes. This bot is small, but it writes into shared business tables, so changes must be narrow and verified.

## Read Order

Infra map (canonical, token-light): /Users/evgenijpastusenko/Projects/agent1/docs/INFRA_MAP_LITE.yaml

1. /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/AGENT_STATE.md
2. recent entries in /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/SESSION_LOG.md
3. `bot.py`
4. `app/db.py`
5. `app/migrations/0003_client_bot.sql`
6. `docs/NEW_BOT_LOGIC.md`
7. `telegram_bot_full_spec.md`
8. `docs/TELEGRAM_BOT_INTEGRATION.md`

## Central Context

This project uses central agent memory outside the current repository.
If `./AGENT_STATE.md` or `./SESSION_LOG.md` are missing here, that is expected.
Read and update only these registered files:

- State: `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/AGENT_STATE.md`
- Log: `/Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/SESSION_LOG.md`

## Key Sources

- `bot.py`
- `app/db.py`
- `app/migrations/0003_client_bot.sql`
- `docs/NEW_BOT_LOGIC.md`
- `telegram_bot_full_spec.md`
- `docs/TELEGRAM_BOT_INTEGRATION.md`

## Working Rules

- Project context state is centralized in /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/AGENT_STATE.md.
- Project session log is centralized in /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/SESSION_LOG.md.
- Central context lives in `agent1/project_ai_context/`, not in this repository.
- Missing local `AGENT_STATE.md` / `SESSION_LOG.md` in `telegram-bot-client` is expected and not an error.
- Do not recreate local AGENT_STATE.md and SESSION_LOG.md in this project.
- If required facts are missing, ask the user directly.
- Do not enumerate speculative options by default.
- Use detective mode only when the user explicitly asks to find a solution or process.

- Treat `bot.py` as the primary runtime entrypoint unless the architecture is explicitly refactored.
- Verify database assumptions against `app/db.py` and migrations before changing stateful flows.
- If docs and code diverge, trust code and note the mismatch in `SESSION_LOG.md`.
- Treat `docs/spec.md` and `docs/dev_guide.md` as low-trust historical artifacts unless confirmed by code.
- Remember this bot touches shared RaketaClean tables; avoid schema or semantic changes without checking wider impact.
- Keep fixes narrow. Do not refactor `bot.py` broadly unless the task truly requires it.
- Record environment-sensitive changes clearly because the project depends on bot tokens and admin IDs.

## Git Hygiene

- Run `git status --short` before editing, before committing, and before deploy.
- Commit completed work in small logical steps.
- Keep bot runtime fixes separate from documentation-only updates when possible.
- Do not deploy or hand off from a dirty worktree if the current task is finished.

## Deploy Rules

- Deploy only from committed state.
- Prefer commit -> push -> deploy -> log verification.
- If the deploy path is not already established for the current environment, stop and document the missing deploy procedure instead of improvising.
- Assume the path is `local -> git -> VPS` unless project docs say otherwise.
- Do not search for passwords, invent credentials, or guess how to get onto the server.
- If SSH works but `sudo` or another privileged step is unavailable, stop and ask the user.

## End Of Session Requirements

Before ending the session:
1. run `git status --short`;
2. commit completed work in one or more small logical commits;
3. rewrite /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/AGENT_STATE.md to reflect current state;
4. append one new entry to /Users/evgenijpastusenko/Projects/agent1/project_ai_context/telegram-bot-client/SESSION_LOG.md;
5. keep both files short, factual, and agent-readable.

## Current Focus

Prepare the project for the next repair session by anchoring work to the live runtime flow: phone capture, signup bonus, questions/orders/media forwarding, and admin notifications.
