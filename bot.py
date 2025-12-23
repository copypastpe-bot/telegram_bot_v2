import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram import BaseMiddleware
from aiogram.types import (
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    TelegramObject,
    User,
)
from dotenv import load_dotenv

from app.db import close_pool, get_pool, init_pool

load_dotenv()
logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

LOGS_CHAT_ID = int(os.getenv("LOGS_CHAT_ID", "0") or "0")
ids_str = os.getenv("ADMIN_TG_IDS", "")
ADMIN_TG_IDS = tuple(int(x) for x in ids_str.split()) if ids_str else ()
ONBOARDING_BONUS = int(os.getenv("ONBOARDING_BONUS", "300") or "300")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

BTN_BONUS = "Мои бонусы"
BTN_ORDER = "Сделать заказ"
BTN_QUESTION = "Задать вопрос"
BTN_SHARE_CONTACT = "📱 Поделиться номером"
BTN_CANCEL = "Отмена"
BTN_PRICE = "💰 Прайс"
BTN_SCHEDULE = "🕐 Режим работы"

# Список всех кнопок меню для проверки
MENU_BUTTONS = [
    BTN_BONUS,
    BTN_ORDER,
    BTN_QUESTION,
    BTN_SHARE_CONTACT,
    BTN_CANCEL,
    BTN_PRICE,
    BTN_SCHEDULE,
]


class ClientRequestFSM(StatesGroup):
    waiting_question = State()
    waiting_order = State()
    waiting_phone_manual = State()


def needs_phone(client: Optional[asyncpg.Record]) -> bool:
    return not (client and client.get("phone"))


def main_menu(require_contact: bool) -> ReplyKeyboardMarkup:
    if require_contact:
        rows = [
            [KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_PRICE), KeyboardButton(text=BTN_SCHEDULE)],
        ]
    else:
        rows = [
            [KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_BONUS)],
            [KeyboardButton(text=BTN_ORDER), KeyboardButton(text=BTN_QUESTION)],
            [KeyboardButton(text=BTN_PRICE), KeyboardButton(text=BTN_SCHEDULE)],
        ]
    rows.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие",
    )


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        input_field_placeholder="Нажмите, чтобы поделиться номером",
    )


async def safe_send_message(chat_id: int, text: str, **kwargs) -> Optional[Message]:
    """
    Безопасная отправка сообщения с автоматической обработкой отписки.
    Возвращает Message при успехе, None при ошибке (включая блокировку бота).
    """
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramBadRequest as e:
        error_codes = {
            "bot_blocked_by_user",
            "user_is_deleted",
            "chat_not_found",
        }
        if any(code in str(e).lower() for code in error_codes):
            await mark_client_unsubscribed(chat_id)
            logging.warning(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")
            return None
        raise
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения пользователю {chat_id}: {e}")
        return None


async def notify_admins(text: str) -> None:
    print(f"[NOTIFY_ADMINS] Вызван. ADMIN_TG_IDS: {ADMIN_TG_IDS}, количество админов: {len(ADMIN_TG_IDS)}")
    print(f"[NOTIFY_ADMINS] Текст сообщения: {text[:100]}...")
    logging.info(f"notify_admins вызван. ADMIN_TG_IDS: {ADMIN_TG_IDS}, количество админов: {len(ADMIN_TG_IDS)}")
    if not ADMIN_TG_IDS:
        print("[NOTIFY_ADMINS] ADMIN_TG_IDS пуст! Сообщение не будет отправлено никому.")
        logging.warning("ADMIN_TG_IDS пуст! Сообщение не будет отправлено никому.")
        return
    for admin_id in ADMIN_TG_IDS:
        try:
            print(f"[NOTIFY_ADMINS] Отправка сообщения админу {admin_id}")
            logging.info(f"Отправка сообщения админу {admin_id}")
            await bot.send_message(admin_id, text)
            print(f"[NOTIFY_ADMINS] Сообщение успешно отправлено админу {admin_id}")
            logging.info(f"Сообщение успешно отправлено админу {admin_id}")
        except Exception as exc:
            print(f"[NOTIFY_ADMINS] Ошибка при отправке админу {admin_id}: {exc}")
            logging.error("Не удалось уведомить админа %s: %s", admin_id, exc)


async def get_bonus_info(conn: asyncpg.Connection, client_id: int) -> Tuple[int, Optional[datetime]]:
    """Получает баланс бонусов и срок действия новых бонусов за подписку."""
    balance = await conn.fetchval(
        "SELECT bonus_balance FROM clients WHERE id=$1",
        client_id
    ) or 0
    
    # Находим срок действия бонусов за подписку (последняя транзакция с expires_at)
    expires_at = await conn.fetchval(
        """
        SELECT expires_at
        FROM bonus_transactions
        WHERE client_id = $1
          AND reason = 'bot_signup'
          AND expires_at IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        client_id
    )
    
    return int(balance), expires_at


async def send_bonus_message(client: asyncpg.Record, user: User) -> None:
    """Отправляет сообщение о начисленных бонусах после получения телефона."""
    pool = get_pool()
    async with pool.acquire() as conn:
        balance, expires_at = await get_bonus_info(conn, client["id"])
    
    lines = [
        "✅ Вам начислено 300 бонусов за подписку! 🎁",
        f"Текущий баланс: <b>{balance}</b> бонусов",
    ]
    
    if expires_at:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        # Конвертируем в МСК для отображения
        from zoneinfo import ZoneInfo
        MOSCOW_TZ = ZoneInfo("Europe/Moscow")
        expires_local = expires_at.astimezone(MOSCOW_TZ)
        expires_str = expires_local.strftime("%d.%m.%Y")
        lines.append(f"Срок действия новых бонусов: до {expires_str}")
    
    lines.extend([
        "",
        "Теперь вам доступны функции:",
        "• Задать вопрос",
        "• Сделать заказ",
        "• Посмотреть бонусы",
    ])
    
    await safe_send_message(user.id, "\n".join(lines), parse_mode=ParseMode.HTML)


async def log_signup(client: asyncpg.Record, user: User, was_merged: bool = False) -> None:
    """Логирует нового подписчика в чат после получения телефона."""
    if LOGS_CHAT_ID == 0:
        return
    username = f"@{user.username}" if user.username else "—"
    phone = client.get("phone") or "не указан"
    merge_note = " (объединен с существующим клиентом)" if was_merged else ""
    text = (
        "🆕 Новый подписчик клиентского бота\n"
        f"ID клиента: {client['id']}\n"
        f"Имя: {client.get('full_name') or client.get('name') or user.full_name}\n"
        f"Телефон: {phone}\n"
        f"Telegram: {username}\n"
        f"TG ID: {user.id}\n"
        f"✅ бонус {ONBOARDING_BONUS} начислен{merge_note}"
    )
    try:
        await bot.send_message(LOGS_CHAT_ID, text)
    except Exception as exc:
        logging.warning("Не удалось отправить лог о подписчике: %s", exc)


def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    if digits.startswith("7") and len(digits) == 11:
        return f"+{digits}"
    if raw.startswith("+"):
        return raw.strip()
    return raw.strip()


_CLIENTS_NAME_COLUMN: str | None = None
_CLIENTS_COLUMNS: set[str] | None = None


async def _clients_columns(conn: asyncpg.Connection) -> set[str]:
    global _CLIENTS_COLUMNS
    if _CLIENTS_COLUMNS is not None:
        return _CLIENTS_COLUMNS
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'clients'
        """
    )
    _CLIENTS_COLUMNS = {str(r["column_name"]) for r in rows if r and r.get("column_name")}
    return _CLIENTS_COLUMNS


async def _clients_has_column(conn: asyncpg.Connection, column_name: str) -> bool:
    return column_name in await _clients_columns(conn)


async def _fetch_client_by_tg(conn: asyncpg.Connection, user_id: int) -> Optional[asyncpg.Record]:
    cols = await _clients_columns(conn)
    clauses: list[str] = []
    order_cases: list[str] = []
    if "bot_tg_user_id" in cols:
        clauses.append("bot_tg_user_id = $1")
        order_cases.append("CASE WHEN bot_tg_user_id = $1 THEN 0 ELSE 1 END")
    if "tg_user_id" in cols:
        clauses.append("tg_user_id = $1")
        order_cases.append("CASE WHEN tg_user_id = $1 THEN 1 ELSE 2 END")
    if not clauses:
        return None
    condition = " OR ".join(clauses)
    order_sql = ", ".join(order_cases) if order_cases else "id"
    sql = f"""
        SELECT *
        FROM clients
        WHERE {condition}
        ORDER BY {order_sql}, id
        LIMIT 1
    """
    return await conn.fetchrow(sql, user_id)


async def _clients_name_column(conn: asyncpg.Connection) -> str:
    """
    Detect whether `clients` table stores name in `full_name` or `name`.
    Supports both schemas (older migrations: `name`, newer/production: `full_name`).
    """
    global _CLIENTS_NAME_COLUMN
    if _CLIENTS_NAME_COLUMN:
        return _CLIENTS_NAME_COLUMN
    if await _clients_has_column(conn, "full_name"):
        _CLIENTS_NAME_COLUMN = "full_name"
        return _CLIENTS_NAME_COLUMN
    if await _clients_has_column(conn, "name"):
        _CLIENTS_NAME_COLUMN = "name"
        return _CLIENTS_NAME_COLUMN

    raise RuntimeError("clients table has neither 'name' nor 'full_name' column")


def normalize_phone_digits(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    return digits


async def _update_client_tg_fields(conn: asyncpg.Connection, client_id: int, user: User) -> asyncpg.Record:
    """
    Best-effort update of telegram identity fields on clients table, if those columns exist.
    Returns updated client row.
    """
    cols = await _clients_columns(conn)
    updates: list[str] = []
    params: list[object] = [client_id]
    idx = 2

    def add(col: str, val: object) -> None:
        nonlocal idx
        updates.append(f"{col} = ${idx}")
        params.append(val)
        idx += 1

    if "status" in cols:
        add("status", "client")
    if "tg_user_id" in cols:
        add("tg_user_id", user.id)
    if "tg_id" in cols:
        add("tg_id", user.id)
    if "tg_username" in cols:
        add("tg_username", user.username)
    if "tg_first_name" in cols:
        add("tg_first_name", user.first_name)
    if "tg_last_name" in cols:
        add("tg_last_name", user.last_name)
    if "tg_language_code" in cols:
        add("tg_language_code", user.language_code)
    if "tg_is_premium" in cols:
        add("tg_is_premium", bool(getattr(user, "is_premium", False)))
    if "last_updated" in cols:
        updates.append("last_updated = NOW()")

    if not updates:
        row = await conn.fetchrow("SELECT * FROM clients WHERE id=$1", client_id)
        if not row:
            raise RuntimeError("Client row not found after update")
        return row

    sql = "UPDATE clients SET " + ", ".join(updates) + f" WHERE id = $1 RETURNING *"
    row = await conn.fetchrow(sql, *params)
    if not row:
        raise RuntimeError("Client row not found after update")
    return row


async def merge_clients(conn: asyncpg.Connection, keep_id: int, drop_id: int) -> None:
    await conn.execute("UPDATE orders SET client_id=$1 WHERE client_id=$2", keep_id, drop_id)
    await conn.execute(
        "UPDATE bonus_transactions SET client_id=$1 WHERE client_id=$2",
        keep_id,
        drop_id,
    )
    await conn.execute(
        """
        UPDATE clients target
        SET bonus_balance = target.bonus_balance + source.bonus_balance,
            total_spent = target.total_spent + source.total_spent,
            total_bonuses_earned = target.total_bonuses_earned + source.total_bonuses_earned,
            total_bonuses_spent = target.total_bonuses_spent + source.total_bonuses_spent
        FROM clients source
        WHERE target.id=$1 AND source.id=$2
        """,
        keep_id,
        drop_id,
    )
    await conn.execute("DELETE FROM clients WHERE id=$1", drop_id)


async def get_client_by_tg(user_id: int) -> Optional[asyncpg.Record]:
    pool = get_pool()
    async with pool.acquire() as conn:
        return await _fetch_client_by_tg(conn, user_id)


async def upsert_contact(user: User, phone_raw: str, name: Optional[str]) -> Tuple[asyncpg.Record, bool]:
    """
    Обрабатывает получение телефона от пользователя.
    Ищет клиента по телефону, объединяет записи если нужно, начисляет бонусы с сроком жизни.
    Возвращает: (client, was_merged) - был ли клиент объединен с существующим по телефону
    """
    phone = normalize_phone(phone_raw)
    phone_digits = normalize_phone_digits(phone)
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            cols = await _clients_columns(conn)
            client_by_tg = await _fetch_client_by_tg(conn, user.id)
            if "phone_digits" in cols and phone_digits:
                client_by_phone = await conn.fetchrow(
                    "SELECT * FROM clients WHERE phone_digits=$1",
                    phone_digits,
                )
            else:
                client_by_phone = await conn.fetchrow(
                    "SELECT * FROM clients WHERE phone=$1",
                    phone,
                )

            was_merged = False
            target_id: Optional[int] = None
            
            if client_by_phone:
                # Клиент найден по телефону - используем его, добавляем недостающие поля
                target_id = client_by_phone["id"]
                was_merged = (client_by_tg is not None and client_by_tg["id"] != client_by_phone["id"])
                
                # Начисляем бонусы за подписку (если еще не начисляли)
                existing_signup_bonus = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM bonus_transactions
                    WHERE client_id = $1 AND reason = 'bot_signup'
                    """,
                    client_by_phone["id"]
                )
                if existing_signup_bonus == 0:
                    expires_at = datetime.now(timezone.utc) + timedelta(days=30)
                    await conn.execute(
                        """
                        UPDATE clients
                        SET bonus_balance = bonus_balance + $1,
                            bot_bonus_granted = true
                        WHERE id=$2
                        """,
                        ONBOARDING_BONUS,
                        client_by_phone["id"],
                    )
                    await conn.execute(
                        """
                        INSERT INTO bonus_transactions(client_id, order_id, delta, reason, expires_at)
                        VALUES ($1, NULL, $2, 'bot_signup', $3)
                        """,
                        client_by_phone["id"],
                        ONBOARDING_BONUS,
                        expires_at,
                    )
                
                # Если был клиент по tg_id (другая запись), переносим данные и удаляем его
                if client_by_tg and client_by_tg["id"] != client_by_phone["id"]:
                    # Переносим заказы и транзакции от клиента по tg_id к клиенту по телефону
                    await conn.execute(
                        "UPDATE orders SET client_id=$1 WHERE client_id=$2",
                        client_by_phone["id"],
                        client_by_tg["id"]
                    )
                    await conn.execute(
                        "UPDATE bonus_transactions SET client_id=$1 WHERE client_id=$2",
                        client_by_phone["id"],
                        client_by_tg["id"]
                    )
                    # Удаляем клиента по tg_id
                    await conn.execute("DELETE FROM clients WHERE id=$1", client_by_tg["id"])
                
                # Обновляем клиента по телефону: добавляем недостающие поля (tg_id, username)
                # НЕ меняем preferred_contact и wahelp_preferred_channel (оставляем как есть)
                updates: list[str] = [
                    "bot_tg_user_id = COALESCE(bot_tg_user_id, $2)",
                    "bot_started = true",
                    "bot_started_at = COALESCE(bot_started_at, now())",
                    "status = 'client'",
                ]
                params: list[object] = [client_by_phone["id"], user.id]
                
                if "tg_user_id" in cols:
                    updates.append("tg_user_id = COALESCE(tg_user_id, $2)")
                
                if "last_updated" in cols:
                    updates.append("last_updated = NOW()")
                
                sql = "UPDATE clients SET " + ", ".join(updates) + " WHERE id=$1 RETURNING *"
                client = await conn.fetchrow(sql, *params)
                if client:
                    try:
                        # Обновляем TG поля (username и другие)
                        client = await _update_client_tg_fields(conn, int(client["id"]), user)
                    except Exception:
                        pass
            elif client_by_tg:
                # Клиент найден по tg_id, но нет по телефону
                target_id = client_by_tg["id"]
                # Обновляем клиента: добавляем телефон и обновляем поля
                # НЕ меняем preferred_contact и wahelp_preferred_channel (оставляем как есть)
                updates: list[str] = [
                    "phone = $2",
                    "bot_started = true",
                    "bot_started_at = COALESCE(bot_started_at, now())",
                    "status = 'client'",
                ]
                params: list[object] = [target_id, phone]
                
                if "tg_user_id" in cols:
                    updates.append("tg_user_id = COALESCE(tg_user_id, $3)")
                    params.append(user.id)
                
                if "last_updated" in cols:
                    updates.append("last_updated = NOW()")
                
                sql = "UPDATE clients SET " + ", ".join(updates) + " WHERE id=$1 RETURNING *"
                client = await conn.fetchrow(sql, *params)
                if client:
                    try:
                        client = await _update_client_tg_fields(conn, int(client["id"]), user)
                    except Exception:
                        pass

            if target_id is None:
                # Новый клиент - создаем запись
                # НЕ устанавливаем preferred_contact (будет NULL или значение по умолчанию)
                name_col = await _clients_name_column(conn)
                has_tg_user_id = await _clients_has_column(conn, "tg_user_id")
                columns = f"{name_col}, phone, status, bot_tg_user_id, bot_started, bot_started_at"
                values = "$1, $2, 'client', $3, true, now()"
                params: list[object] = [
                    name or user.full_name or user.username or "Без имени",
                    phone,
                    user.id,
                ]
                if has_tg_user_id:
                    columns += ", tg_user_id"
                    values += ", $3"
                sql = f"INSERT INTO clients({columns}) VALUES ({values}) RETURNING *"
                client = await conn.fetchrow(sql, *params)
                # Начисляем бонусы за подписку новому клиенту
                expires_at = datetime.now(timezone.utc) + timedelta(days=30)
                await conn.execute(
                    """
                    UPDATE clients
                    SET bonus_balance = bonus_balance + $1,
                        bot_bonus_granted = true
                    WHERE id=$2
                    """,
                    ONBOARDING_BONUS,
                    client["id"],
                )
                await conn.execute(
                    """
                    INSERT INTO bonus_transactions(client_id, order_id, delta, reason, expires_at)
                    VALUES ($1, NULL, $2, 'bot_signup', $3)
                    """,
                    client["id"],
                    ONBOARDING_BONUS,
                    expires_at,
                )
            
            # Обновляем client после всех операций (получаем актуальные данные)
            if target_id:
                client = await conn.fetchrow("SELECT * FROM clients WHERE id=$1", target_id)
            return client, was_merged


def format_admin_payload(kind: str, message: Message, client: Optional[asyncpg.Record]) -> str:
    user = message.from_user
    phone = client["phone"] if client and client.get("phone") else "не указан"
    username = f"@{user.username}" if user.username else "—"
    lines = [
        f"📩 {kind}",
        f"Имя: {user.full_name}",
        f"Username: {username}",
        f"TG ID: {user.id}",
        f"Телефон: {phone}",
        "",
        message.text or "—",
    ]
    return "\n".join(lines)


def is_menu_button(text: str) -> bool:
    """Проверяет, является ли текст кнопкой меню."""
    if not text:
        return False
    
    text_normalized = text.strip()
    
    # Проверяем команды
    if text_normalized.startswith("/"):
        return True
    
    # Проверяем кнопки (убираем эмодзи для сравнения)
    for button in MENU_BUTTONS:
        # Убираем эмодзи и пробелы для сравнения
        button_text = button.split(" ", 1)[-1] if " " in button else button
        if text_normalized.lower() == button.lower() or text_normalized.lower() == button_text.lower():
            return True
    
    return False


async def create_lead_and_notify_admin(message: Message) -> None:
    """Создает лид в БД и отправляет уведомление админам."""
    if not message.from_user:
        return
    
    user = message.from_user
    pool = get_pool()
    async with pool.acquire() as conn:
        # Проверяем структуру таблицы leads
        cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'leads'
            """
        )
        has_tg_user_id = any(col["column_name"] == "tg_user_id" for col in cols)
        
        # Проверяем, есть ли уже лид с таким tg_user_id (если колонка есть)
        existing_lead = None
        if has_tg_user_id:
            existing_lead = await conn.fetchrow(
                """
                SELECT id FROM leads
                WHERE tg_user_id = $1
                LIMIT 1
                """,
                user.id
            )
        
        if not existing_lead:
            # Создаем новый лид
            if has_tg_user_id:
                await conn.execute(
                    """
                    INSERT INTO leads(name, phone, source, status, tg_user_id)
                    VALUES ($1, NULL, 'telegram_bot', 'new', $2)
                    """,
                    user.full_name or user.username or "Без имени",
                    user.id
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO leads(name, phone, source, status)
                    VALUES ($1, NULL, 'telegram_bot', 'new')
                    """,
                    user.full_name or user.username or "Без имени"
                )
        
        # Отправляем админам
        payload = format_admin_payload("Вопрос от лида (без телефона)", message, None)
        await notify_admins(payload)


async def send_menu(message: Message, client: Optional[asyncpg.Record]) -> None:
    await message.answer(
        "Главное меню RaketaClean",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    print(f"[START_HANDLER] Обработка команды /start от {message.from_user.id if message.from_user else 'unknown'}")
    await state.clear()
    if not message.from_user:
        return
    
    # Проверяем, есть ли уже клиент по tg_id
    client = await get_client_by_tg(message.from_user.id)
    
    if client:
        # Клиент уже есть - показываем меню без запроса телефона
        phone_required = needs_phone(client)
        if phone_required:
            # Есть клиент, но нет телефона - просим телефон
            await message.answer(
                "Привет! 👋\n\n"
                "⚠️ <b>Важно:</b> Чтобы пользоваться ботом, нужно указать номер телефона.\n"
                "Поделитесь номером через кнопку ниже или введите его вручную (формат: 9XXXXXXXXX).",
                reply_markup=main_menu(require_contact=True),
                parse_mode=ParseMode.HTML,
            )
        else:
            # Есть клиент с телефоном - показываем обычное меню
            await message.answer(
                "Привет! 👋\n\n"
                "Выберите действие через меню:",
                reply_markup=main_menu(require_contact=False),
                parse_mode=ParseMode.HTML,
            )
    else:
        # Новый клиент - показываем приветствие с запросом телефона
        await message.answer(
            "Привет! 👋\n\n"
            "Этот бот будет присылать бонусы, акции и напоминания от RaketaClean.\n\n"
            "⚠️ <b>Важно:</b> Чтобы пользоваться ботом, нужно указать номер телефона.\n"
            "Поделитесь номером через кнопку ниже или введите его вручную (формат: 9XXXXXXXXX).",
            reply_markup=main_menu(require_contact=True),
            parse_mode=ParseMode.HTML,
        )


@dp.message(F.contact)
async def contact_handler(message: Message, state: FSMContext) -> None:
    print(f"[CONTACT_HANDLER] Обработка контакта от {message.from_user.id if message.from_user else 'unknown'}")
    contact = message.contact
    user = message.from_user
    if not contact or not user:
        return
    if contact.user_id and contact.user_id != user.id:
        await message.answer(
            "Пожалуйста, поделитесь собственным номером через кнопку.",
            reply_markup=contact_keyboard(),
        )
        return
    # aiogram Contact has no `full_name`; use user.full_name or contact's first/last name
    contact_name = None
    first = getattr(contact, "first_name", None)
    last = getattr(contact, "last_name", None)
    if first or last:
        contact_name = " ".join([p for p in [first, last] if p])
    
    client, was_merged = await upsert_contact(user, contact.phone_number, contact_name or user.full_name)
    await state.clear()
    
    # Отправляем сообщение о бонусах
    await send_bonus_message(client, user)
    
    # Уведомляем в чат о новом подписчике
    await log_signup(client, user, was_merged)
    
    await message.answer(
        "Спасибо! Номер сохранён. Теперь можете пользоваться меню.",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(Command("info"))
async def info_handler(message: Message) -> None:
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    await message.answer(
        "Я могу показать бонусы или передать ваше сообщение администратору.",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(StateFilter(ClientRequestFSM.waiting_question))
async def handle_question_text(message: Message, state: FSMContext) -> None:
    print(f"[HANDLE_QUESTION_TEXT] Обработка текста в состоянии waiting_question от {message.from_user.id if message.from_user else 'unknown'}: {message.text[:50] if message.text else 'no text'}")
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    payload = format_admin_payload("Вопрос от клиента", message, client)
    await notify_admins(payload)
    await message.answer(
        "Передал вопрос администратору. Ответим как можно скорее!",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )
    await state.clear()


@dp.message(StateFilter(ClientRequestFSM.waiting_order))
async def handle_order_text(message: Message, state: FSMContext) -> None:
    print(f"[HANDLE_ORDER_TEXT] Обработка текста в состоянии waiting_order от {message.from_user.id if message.from_user else 'unknown'}: {message.text[:50] if message.text else 'no text'}")
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    payload = format_admin_payload("Заявка на заказ", message, client)
    await notify_admins(payload)
    await message.answer(
        "Заказ передан администратору. Мы свяжемся, чтобы уточнить детали.",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )
    await state.clear()


@dp.message(F.text.casefold() == BTN_BONUS.lower())
async def bonuses_handler(message: Message) -> None:
    print(f"[BONUSES_HANDLER] Обработка кнопки 'Мои бонусы' от {message.from_user.id if message.from_user else 'unknown'}")
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    if not client:
        await message.answer(
            "Не нашёл ваш профиль. Напишите администратору или попробуйте позже.",
            reply_markup=main_menu(require_contact=True),
        )
        return
    if needs_phone(client):
        await message.answer(
            "Бонусы отображаются после подтверждения номера. Нажмите «Поделиться номером».",
            reply_markup=contact_keyboard(),
        )
        return
    balance = client.get("bonus_balance") or 0
    await message.answer(
        f"На вашем бонусном счету <b>{balance}</b> бонусов. Можно оплатить ими до 50% заказа.",
        reply_markup=main_menu(require_contact=False),
    )


@dp.message(F.text.casefold() == BTN_SHARE_CONTACT.lower())
async def share_contact_prompt(message: Message, state: FSMContext) -> None:
    print(f"[SHARE_CONTACT_PROMPT] Обработка кнопки 'Поделиться номером' от {message.from_user.id if message.from_user else 'unknown'}")
    await state.set_state(ClientRequestFSM.waiting_phone_manual)
    await message.answer(
        "Нажмите кнопку ниже, чтобы отправить номер автоматически.\n\n"
        "Или введите номер вручную в формате: <b>9XXXXXXXXX</b> (10 цифр, начинается с 9)",
        reply_markup=contact_keyboard(),
    )


@dp.message(F.text.casefold() == BTN_QUESTION.lower())
async def ask_question(message: Message, state: FSMContext) -> None:
    print(f"[ASK_QUESTION] Обработка кнопки 'Задать вопрос' от {message.from_user.id if message.from_user else 'unknown'}")
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    if needs_phone(client):
        await message.answer(
            "⚠️ Сначала нужно указать номер телефона. Поделитесь номером через кнопку или введите его вручную.",
            reply_markup=contact_keyboard(),
        )
        return
    await state.set_state(ClientRequestFSM.waiting_question)
    await message.answer(
        "Опишите ваш вопрос. Чтобы отменить, напишите «Отмена».",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(F.text.casefold() == BTN_ORDER.lower())
async def make_order(message: Message, state: FSMContext) -> None:
    print(f"[MAKE_ORDER] Обработка кнопки 'Сделать заказ' от {message.from_user.id if message.from_user else 'unknown'}")
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    if needs_phone(client):
        await message.answer(
            "⚠️ Сначала нужно указать номер телефона. Поделитесь номером через кнопку или введите вручную.",
            reply_markup=contact_keyboard(),
        )
        return
    await state.set_state(ClientRequestFSM.waiting_order)
    await message.answer(
        "Расскажите, какая услуга нужна. Пока просто передаю текст администратору.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(F.text.casefold() == BTN_PRICE.lower())
async def price_handler(message: Message) -> None:
    """Обработчик кнопки 'Прайс' - показывает ссылку на прайс на сайте"""
    print(f"[PRICE_HANDLER] Обработка кнопки 'Прайс' от {message.from_user.id if message.from_user else 'unknown'}")
    text = "💰 <b>Прайс на услуги</b>\n\nПосмотрите актуальные цены на нашем сайте:"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Открыть прайс", url="https://raketaclean.ru/price")]
        ]
    )
    await message.answer(text, reply_markup=keyboard)


@dp.message(F.text.casefold() == BTN_SCHEDULE.lower())
async def schedule_handler(message: Message) -> None:
    """Обработчик кнопки 'Режим работы' - показывает контактную информацию"""
    print(f"[SCHEDULE_HANDLER] Обработка кнопки 'Режим работы' от {message.from_user.id if message.from_user else 'unknown'}")
    text = (
        "🕐 <b>Режим работы:</b>\n"
        "Ежедневно с 9:00 до 19:00\n\n"
        "<b>Для связи:</b>\n"
        "Телефон: +79040437523\n"
        "Telegram: @raketaclean\n"
        "Сайт: raketaclean.ru\n"
        "Эл.почта: raketa@raketaclean.ru\n"
        "Адрес: Нижний Новгород, ул. Артельная 37 (офис)\n\n"
        "<b>Услуги:</b> Химчистка мебели и ковролина, клининг, стирка ковров, клининг для бизнеса"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


async def mark_client_unsubscribed(user_id: int) -> None:
    """Помечает клиента как отписавшегося от бота."""
    pool = get_pool()
    async with pool.acquire() as conn:
        client = await _fetch_client_by_tg(conn, user_id)
        if not client:
            return
        cols = await _clients_columns(conn)
        updates: list[str] = []
        params: list[object] = [client["id"]]
        idx = 2

        def add(col: str, value: object) -> None:
            nonlocal idx
            updates.append(f"{col} = ${idx}")
            params.append(value)
            idx += 1

        if "bot_started" in cols:
            add("bot_started", False)
        if "preferred_contact" in cols:
            add("preferred_contact", "wahelp")

        if not updates:
            return
        set_clause = ", ".join(updates)
        await conn.execute(f"UPDATE clients SET {set_clause} WHERE id=$1", *params)
        logging.info(f"Клиент {client['id']} (TG: {user_id}) помечен как отписавшийся")


async def mark_client_subscribed(user_id: int) -> None:
    """Помечает клиента как подписавшегося на бота."""
    pool = get_pool()
    async with pool.acquire() as conn:
        client = await _fetch_client_by_tg(conn, user_id)
        if not client:
            return
        cols = await _clients_columns(conn)
        updates: list[str] = []
        literals: list[str] = []
        params: list[object] = [client["id"]]
        idx = 2

        def add(col: str, value: object) -> None:
            nonlocal idx
            updates.append(f"{col} = ${idx}")
            params.append(value)
            idx += 1

        if "bot_started" in cols:
            add("bot_started", True)
        if "preferred_contact" in cols:
            add("preferred_contact", "bot")
        if "bot_started_at" in cols:
            literals.append("bot_started_at = COALESCE(bot_started_at, NOW())")

        if not updates and not literals:
            return
        set_clause_parts = updates + literals
        set_clause = ", ".join(set_clause_parts)
        await conn.execute(f"UPDATE clients SET {set_clause} WHERE id=$1", *params)
        logging.info(f"Клиент {client['id']} (TG: {user_id}) помечен как подписавшийся")


class UnsubscribeMiddleware(BaseMiddleware):
    """Middleware для обработки отписки пользователей при ошибках отправки сообщений."""
    
    async def __call__(
        self,
        handler,
        event: TelegramObject,
        data: dict,
    ):
        try:
            return await handler(event, data)
        except TelegramBadRequest as e:
            # Обрабатываем ошибки, связанные с блокировкой бота
            error_message = str(e).lower()
            error_codes = [
                "bot_blocked_by_user",
                "user_is_deleted", 
                "chat_not_found",
                "user not found",
            ]
            
            if any(code in error_message for code in error_codes):
                # Пытаемся получить user_id из события
                user_id = None
                if isinstance(event, Message):
                    if event.from_user:
                        user_id = event.from_user.id
                    elif event.chat and event.chat.type == ChatType.PRIVATE:
                        user_id = event.chat.id
                elif isinstance(event, ChatMemberUpdated):
                    if event.from_user:
                        user_id = event.from_user.id
                    elif event.chat and event.chat.type == ChatType.PRIVATE:
                        user_id = event.chat.id
                
                if user_id:
                    await mark_client_unsubscribed(user_id)
                    logging.warning(f"Пользователь {user_id} заблокировал бота или удалён: {e}")
                else:
                    logging.warning(f"Не удалось определить user_id для обработки отписки: {e}")
            
            # Пробрасываем ошибку дальше, если это не связано с блокировкой
            if not any(code in error_message for code in error_codes):
                raise


@dp.my_chat_member()
async def chat_member_updates(event: ChatMemberUpdated) -> None:
    """Обработка событий изменения статуса в группах/каналах."""
    if event.chat.type != ChatType.PRIVATE:
        return
    user = event.new_chat_member.user
    if not user:
        return
    status = event.new_chat_member.status
    
    if status in {ChatMemberStatus.KICKED, ChatMemberStatus.LEFT}:
        await mark_client_unsubscribed(user.id)
    elif status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR}:
        await mark_client_subscribed(user.id)


@dp.message(Command("cancel"))
@dp.message(F.text.casefold() == BTN_CANCEL.lower())
async def cancel_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    await message.answer(
        "Ок, вернулись в главное меню.",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(StateFilter(ClientRequestFSM.waiting_phone_manual), F.text)
async def handle_manual_phone(message: Message, state: FSMContext) -> None:
    """Обработка ручного ввода номера телефона (только текстовые сообщения)."""
    print(f"[HANDLE_MANUAL_PHONE] Обработка текста в состоянии waiting_phone_manual от {message.from_user.id if message.from_user else 'unknown'}: {message.text[:50] if message.text else 'no text'}")
    if not message.from_user:
        return

    phone_text = (message.text or "").strip()
    if not phone_text:
        await message.answer(
            "Пожалуйста, отправьте номер <b>текстом</b> в формате <b>9XXXXXXXXX</b> "
            "или нажмите кнопку ниже, чтобы поделиться номером автоматически.",
            reply_markup=contact_keyboard(),
        )
        return

    # Проверяем формат: 9XXXXXXXXX (10 цифр, начинается с 9)
    if re.match(r"^9\d{9}$", phone_text):
        normalized = normalize_phone(phone_text)
        user = message.from_user
        client, was_merged = await upsert_contact(user, normalized, user.full_name)
        await state.clear()
        
        # Отправляем сообщение о бонусах
        await send_bonus_message(client, user)
        
        # Уведомляем в чат о новом подписчике
        await log_signup(client, user, was_merged)
        
        await message.answer(
            f"✅ Номер {normalized} сохранён! Теперь можете пользоваться всеми функциями бота.",
            reply_markup=main_menu(require_contact=needs_phone(client)),
        )
        return

    await message.answer(
        "❌ Неверный формат номера. Введите номер в формате: <b>9XXXXXXXXX</b> (10 цифр, начинается с 9)\n\n"
        "Или нажмите кнопку ниже, чтобы поделиться номером автоматически.",
        reply_markup=contact_keyboard(),
    )


@dp.message(StateFilter(ClientRequestFSM.waiting_phone_manual))
async def handle_manual_phone_nontext(message: Message, state: FSMContext) -> None:
    """Защита от не-текстовых сообщений в режиме ручного ввода номера."""
    print(f"[HANDLE_MANUAL_PHONE_NONTEXT] Обработка не-текста в состоянии waiting_phone_manual от {message.from_user.id if message.from_user else 'unknown'}")
    if not message.from_user:
        return
    # Контакт обработает отдельный хэндлер F.contact
    if message.contact:
        return
    await message.answer(
        "Пожалуйста, отправьте номер <b>текстом</b> в формате <b>9XXXXXXXXX</b> "
        "или нажмите кнопку ниже, чтобы поделиться номером автоматически.",
        reply_markup=contact_keyboard(),
    )


@dp.message()
async def fallback(message: Message, state: FSMContext) -> None:
    """Обработчик всех сообщений, которые не попали в другие handlers."""
    # Используем print для гарантированного вывода в логи
    print(f"[FALLBACK] Handler вызван для сообщения от {message.from_user.id if message.from_user else 'unknown'}: {message.text[:50] if message.text else 'no text'}")
    logging.info(f"[FALLBACK] Handler вызван для сообщения от {message.from_user.id if message.from_user else 'unknown'}: {message.text[:50] if message.text else 'no text'}")
    
    current_state = await state.get_state()
    if current_state:
        logging.info(f"Пользователь в состоянии FSM: {current_state}")
        await message.answer("Пожалуйста, завершите текущий шаг или напишите «Отмена».")
        return
    
    if not message.from_user or not message.text:
        logging.info("Нет пользователя или текста в сообщении")
        return
    
    # Проверяем, является ли это кнопкой меню
    if is_menu_button(message.text):
        logging.info(f"Текст '{message.text}' распознан как кнопка меню")
        # Это кнопка меню, но не обработалась другим handler'ом
        # Просто показываем меню
        client = await get_client_by_tg(message.from_user.id)
        await message.answer(
            "Выберите действие через меню: бонусы, заказ или вопрос.",
            reply_markup=main_menu(require_contact=needs_phone(client)),
        )
        return
    
    # Это произвольное текстовое сообщение
    print(f"[FALLBACK] Обработка произвольного текста от пользователя {message.from_user.id}")
    logging.info(f"Обработка произвольного текста от пользователя {message.from_user.id}")
    client = await get_client_by_tg(message.from_user.id)
    print(f"[FALLBACK] Клиент найден: {client is not None}, нужен телефон: {needs_phone(client) if client else 'N/A'}")
    logging.info(f"Клиент найден: {client is not None}, нужен телефон: {needs_phone(client) if client else 'N/A'}")
    
    if needs_phone(client):
        # Клиент без телефона - создаем лид и отправляем админу
        print("[FALLBACK] Создание лида и отправка админу для клиента без телефона")
        logging.info("Создание лида и отправка админу для клиента без телефона")
        await create_lead_and_notify_admin(message)
        await message.answer(
            "Сообщение передано менеджеру. Мы свяжемся с вами в ближайшее время.\n\n"
            "⚠️ Для полного доступа к функциям бота укажите номер телефона.",
            reply_markup=main_menu(require_contact=True),
        )
    else:
        # Клиент с телефоном - отправляем как вопрос админу
        print(f"[FALLBACK] Отправка вопроса админу для клиента с телефоном. ADMIN_TG_IDS: {ADMIN_TG_IDS}")
        logging.info(f"Отправка вопроса админу для клиента с телефоном. ADMIN_TG_IDS: {ADMIN_TG_IDS}")
        payload = format_admin_payload("Вопрос от клиента", message, client)
        await notify_admins(payload)
        print("[FALLBACK] Вопрос отправлен админам")
        logging.info("Вопрос отправлен админам")
        await message.answer(
            "Передал вопрос администратору. Ответим как можно скорее!",
            reply_markup=main_menu(require_contact=False),
        )


async def main() -> None:
    # Регистрируем middleware для обработки отписки
    # В aiogram 3.x middleware регистрируется через update
    dp.update.middleware(UnsubscribeMiddleware())
    
    await init_pool(min_size=1, max_size=5)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
