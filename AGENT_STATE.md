# AGENT_STATE

project: telegram-bot-client
last_updated: 2026-08-13
updated_by: claude (agent1 maintenance pass)
status: active
confidence: medium

## Purpose

`telegram-bot-client` is a small client-facing Telegram bot for RaketaClean. It is a companion project to the broader RaketaClean bot ecosystem and handles onboarding, phone capture, signup bonus logic, simple client interactions, and admin notifications.

## Current State

- Branch `main` at `9db53c0`; **4 commits ahead of `origin/main`, none pushed**; worktree clean (verified 2026-08-13).
- The 4 unpushed commits are documentation/agent-instruction changes made during the `agent1` cleanup of 2026-05-06, not runtime changes.
- Last commit of any kind is 2026-05-06: no implementation work for ~3 months despite `status: active` and a 1-day verification interval.
- Runtime is concentrated in `bot.py` and runs by long polling.
- Flow: ask for a phone number, link or create a client in the shared database, grant a one-time signup bonus, allow the user to ask a question, request an order, send media for evaluation, view bonus balance, read static price/work-schedule info.
- Writes into shared business tables (`clients`, `bonus_transactions`, `orders`, `leads`), so changes here can affect the wider RaketaClean stack.
- DB pool helper is `app/db.py`; the only local SQL migration present is `app/migrations/0003_client_bot.sql`.
- `bot.py` includes a Telegram API IP fallback resolver modeled on `tgbot-v1`, plus a periodic shared-DB heartbeat that probes Telegram API reachability and reports service health to the admin bot.
- Source of truth for behaviour: `bot.py`, `app/db.py`, `app/migrations/0003_client_bot.sql`, `docs/NEW_BOT_LOGIC.md`, `telegram_bot_full_spec.md`, `docs/TELEGRAM_BOT_INTEGRATION.md`.

## Pending

- Push the 4 local commits to `origin/main` or decide to discard them.
- Deploy and verify both the Telegram API IP fallback and the shared heartbeat so long-polling outages surface quickly in the admin bot; this was the active focus recorded on 2026-04-13 and has not been confirmed since.
- Owner decision: confirm whether `status: active` still reflects intent, or move the project to `paused` in `registry.yaml`.

## Known Limitations

- No deploy or runtime verification has been recorded since 2026-04-13; operational facts below are carried over, not re-verified.
- `bot.py` is the runtime truth and the surrounding docs are partly stale or contradictory.
- `docs/spec.md` is effectively empty and `docs/dev_guide.md` describes an older or different project shape.
- `telegram_bot_full_spec.md` and amoCRM-related docs contain useful business context but must not be treated as final implementation truth.
- The bot modifies shared RaketaClean tables, so schema or behavioral changes should be checked against the wider ecosystem, especially `tgbot-v1`.
- The fallback uses a baked-in Telegram IP pool unless env overrides are set; if Telegram rotates reachable ingress IPs, the pool may need refresh.
- Heartbeat-based alerting works only when this repo and `tgbot-v1` are deployed with the same shared `service_heartbeats` contract.
