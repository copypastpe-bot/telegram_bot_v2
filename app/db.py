import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DB_DSN = os.getenv("DB_DSN")
_pool: asyncpg.Pool | None = None

async def init_pool(min_size: int = 1, max_size: int = 5) -> asyncpg.Pool:
    global _pool
    if not DB_DSN:
        raise RuntimeError("DB_DSN is not set in .env")
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=min_size, max_size=max_size)
    return _pool

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_pool() first.")
    return _pool

async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
