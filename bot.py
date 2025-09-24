import asyncio, os, logging
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")

bot = Bot(TOKEN)
dp = Dispatcher()

@dp.message(F.text == "/start")
async def start(msg: Message):
    await msg.answer("Привет! v2 на связи ✅")

@dp.message(F.text == "/ping")
async def ping(msg: Message):
    await msg.answer("pong")

async def main():
    logging.info("Starting polling…")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
