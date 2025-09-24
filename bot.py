import asyncio, os, logging
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    logging.error("BOT_TOKEN is not set. Create .env with BOT_TOKEN=<token> (see .env.example). Exiting.")
    raise SystemExit(2)

bot = Bot(TOKEN)
dp = Dispatcher()

@dp.message(F.text == "/start")
async def start(msg: Message):
    await msg.answer("Привет! v2 на связи ✅")

@dp.message(F.text == "/ping")
async def ping(msg: Message):
    await msg.answer("pong")

@dp.message(F.text == "/id")
async def whoami(msg: Message):
    await msg.answer(f"Твой Telegram ID: {msg.from_user.id}")

@dp.message(F.text == "/help")
async def help(msg: Message):
    await msg.answer("Список команд:\n/start - начать работу\n/ping - проверить бота\n/id - узнать свой Telegram ID\n/help - список команд")

async def main():
    logging.info("Starting polling…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
