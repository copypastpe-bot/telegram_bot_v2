import asyncio
import logging
import os
import re
from typing import Optional, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
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


class ClientRequestFSM(StatesGroup):
    waiting_question = State()
    waiting_order = State()
    waiting_phone_manual = State()


def needs_phone(client: Optional[asyncpg.Record]) -> bool:
    return not (client and client.get("phone"))


def main_menu(require_contact: bool) -> ReplyKeyboardMarkup:
    rows = []
    if require_contact:
        rows.append([KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)])
    rows.append([KeyboardButton(text=BTN_BONUS)])
    rows.append([KeyboardButton(text=BTN_ORDER), KeyboardButton(text=BTN_QUESTION)])
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


async def notify_admins(text: str) -> None:
    for admin_id in ADMIN_TG_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as exc:
            logging.error("Не удалось уведомить админа %s: %s", admin_id, exc)


async def log_signup(client: asyncpg.Record, user: User, bonus_awarded: bool, newly_started: bool) -> None:
    if LOGS_CHAT_ID == 0 or not newly_started:
        return
    username = f"@{user.username}" if user.username else "—"
    phone = client.get("phone") or "не указан"
    bonus_line = f"✅ бонус {ONBOARDING_BONUS} начислен" if bonus_awarded else "ℹ️ бонус уже выдавался"
    text = (
        "🆕 Новый подписчик клиентского бота\n"
        f"ID клиента: {client['id']}\n"
        f"Имя: {client.get('full_name') or client.get('name') or user.full_name}\n"
        f"Телефон: {phone}\n"
        f"Telegram: {username}\n"
        f"TG ID: {user.id}\n"
        f"{bonus_line}"
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


async def _clients_name_column(conn: asyncpg.Connection) -> str:
    """
    Detect whether `clients` table stores name in `full_name` or `name`.
    Supports both schemas (older migrations: `name`, newer/production: `full_name`).
    """
    global _CLIENTS_NAME_COLUMN
    if _CLIENTS_NAME_COLUMN:
        return _CLIENTS_NAME_COLUMN

    has_full_name = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'clients'
          AND column_name = 'full_name'
        LIMIT 1
        """
    )
    if has_full_name:
        _CLIENTS_NAME_COLUMN = "full_name"
        return _CLIENTS_NAME_COLUMN

    has_name = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'clients'
          AND column_name = 'name'
        LIMIT 1
        """
    )
    if has_name:
        _CLIENTS_NAME_COLUMN = "name"
        return _CLIENTS_NAME_COLUMN

    raise RuntimeError("clients table has neither 'name' nor 'full_name' column")


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


async def ensure_client(user: User) -> Tuple[asyncpg.Record, bool, bool]:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            client = await conn.fetchrow(
                "SELECT * FROM clients WHERE bot_tg_user_id=$1",
                user.id,
            )
            newly_started = False
            if client:
                if not client["bot_started"]:
                    newly_started = True
                    client = await conn.fetchrow(
                        """
                        UPDATE clients
                        SET bot_started = true,
                            bot_started_at = COALESCE(bot_started_at, now()),
                            preferred_contact = 'bot'
                        WHERE id=$1
                        RETURNING *
                        """,
                        client["id"],
                    )
                elif client.get("preferred_contact") != "bot":
                    client = await conn.fetchrow(
                        "UPDATE clients SET preferred_contact='bot' WHERE id=$1 RETURNING *",
                        client["id"],
                    )
            else:
                newly_started = True
                name_col = await _clients_name_column(conn)
                client = await conn.fetchrow(
                    f"""
                    INSERT INTO clients({name_col}, phone, status, bot_tg_user_id, bot_started, bot_started_at, preferred_contact)
                    VALUES ($1, NULL, 'active', $2, true, now(), 'bot')
                    RETURNING *
                    """,
                    user.full_name or user.username or "Без имени",
                    user.id,
                )

            bonus_awarded = False
            if client and not client["bot_bonus_granted"]:
                bonus_awarded = True
                client = await conn.fetchrow(
                    """
                    UPDATE clients
                    SET bonus_balance = bonus_balance + $1,
                        bot_bonus_granted = true
                    WHERE id=$2
                    RETURNING *
                    """,
                    ONBOARDING_BONUS,
                    client["id"],
                )
                await conn.execute(
                    """
                    INSERT INTO bonus_transactions(client_id, order_id, delta, reason)
                    VALUES ($1, NULL, $2, 'bot_signup')
                    """,
                    client["id"],
                    ONBOARDING_BONUS,
                )
            return client, newly_started, bonus_awarded


async def get_client_by_tg(user_id: int) -> Optional[asyncpg.Record]:
    pool = get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM clients WHERE bot_tg_user_id=$1",
            user_id,
        )


async def upsert_contact(user: User, phone_raw: str, name: Optional[str]) -> asyncpg.Record:
    phone = normalize_phone(phone_raw)
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            client_by_tg = await conn.fetchrow(
                "SELECT * FROM clients WHERE bot_tg_user_id=$1",
                user.id,
            )
            client_by_phone = await conn.fetchrow(
                "SELECT * FROM clients WHERE phone=$1",
                phone,
            )

            target_id: Optional[int] = None
            if client_by_tg and client_by_phone and client_by_tg["id"] != client_by_phone["id"]:
                await merge_clients(conn, client_by_phone["id"], client_by_tg["id"])
                target_id = client_by_phone["id"]
            elif client_by_phone:
                target_id = client_by_phone["id"]
            elif client_by_tg:
                target_id = client_by_tg["id"]

            if target_id is None:
                name_col = await _clients_name_column(conn)
                client = await conn.fetchrow(
                    f"""
                    INSERT INTO clients({name_col}, phone, status, bot_tg_user_id, bot_started, bot_started_at, preferred_contact)
                    VALUES ($1, $2, 'active', $3, true, now(), 'bot')
                    RETURNING *
                    """,
                    name or user.full_name or user.username or "Без имени",
                    phone,
                    user.id,
                )
            else:
                client = await conn.fetchrow(
                    """
                    UPDATE clients
                    SET phone = COALESCE($2, phone),
                        bot_tg_user_id = COALESCE(bot_tg_user_id, $3),
                        bot_started = true,
                        bot_started_at = COALESCE(bot_started_at, now()),
                        preferred_contact = 'bot'
                    WHERE id=$1
                    RETURNING *
                    """,
                    target_id,
                    phone,
                    user.id,
                )
            return client


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


async def send_menu(message: Message, client: Optional[asyncpg.Record]) -> None:
    await message.answer(
        "Главное меню RaketaClean",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not message.from_user:
        return
    client, newly_started, bonus_awarded = await ensure_client(message.from_user)
    await log_signup(client, message.from_user, bonus_awarded, newly_started)

    base_text = [
        "Привет! 👋",
        "Этот бот будет присылать бонусы, акции и напоминания от RaketaClean.",
    ]
    if bonus_awarded:
        base_text.append(f"Мы начислили {ONBOARDING_BONUS} бонусов за подписку 🎁")
    elif client.get("bot_bonus_granted"):
        base_text.append("Бонус за подписку уже начислялся ранее.")

    if needs_phone(client):
        base_text.append("⚠️ <b>Важно:</b> Чтобы пользоваться ботом, нужно указать номер телефона.")
        base_text.append("Поделитесь номером через кнопку ниже или введите его вручную (формат: 9XXXXXXXXX).")
    else:
        base_text.append("Можно посмотреть бонусы или отправить запрос администратору.")

    await message.answer(
        "\n\n".join(base_text),
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


@dp.message(F.contact)
async def contact_handler(message: Message, state: FSMContext) -> None:
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
    client = await upsert_contact(user, contact.phone_number, contact.full_name)
    await state.clear()
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
    await state.set_state(ClientRequestFSM.waiting_phone_manual)
    await message.answer(
        "Нажмите кнопку ниже, чтобы отправить номер автоматически.\n\n"
        "Или введите номер вручную в формате: <b>9XXXXXXXXX</b> (10 цифр, начинается с 9)",
        reply_markup=contact_keyboard(),
    )


@dp.message(F.text.casefold() == BTN_QUESTION.lower())
async def ask_question(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    if needs_phone(client):
        await message.answer(
            "⚠️ Сначала нужно указать номер телефона. Поделитесь номером через кнопку или введите вручную.",
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
        client = await upsert_contact(user, normalized, user.full_name)
        await state.clear()
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
    if await state.get_state():
        await message.answer("Пожалуйста, завершите текущий шаг или напишите «Отмена».")
        return
    if not message.from_user:
        return
    client = await get_client_by_tg(message.from_user.id)
    if needs_phone(client):
        await message.answer(
            "⚠️ Сначала нужно указать номер телефона. Поделитесь номером через кнопку или введите вручную.",
            reply_markup=contact_keyboard(),
        )
        return
    await message.answer(
        "Выберите действие через меню: бонусы, заказ или вопрос.",
        reply_markup=main_menu(require_contact=needs_phone(client)),
    )


async def main() -> None:
    await init_pool(min_size=1, max_size=5)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
