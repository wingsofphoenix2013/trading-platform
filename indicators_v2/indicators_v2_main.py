import asyncio
import os
import json
import logging
import asyncpg
import redis.asyncio as aioredis
import numpy as np
import pandas as pd
import pandas_ta as pta
import ta
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from typing import Dict, Any

# 🔸 Конфигурация логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# 🔸 Флаг режима отладки
DEBUG_MODE = False  # Включай True при разработке

def debug_log(message: str):
    if DEBUG_MODE:
        logging.info(message)

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# 🔸 In-memory хранилища
tickers_storage: Dict[str, Dict[str, int]] = {}
ohlcv_cache: Dict[str, Dict[str, Any]] = {}

# 🔸 Подключение к PostgreSQL (асинхронный пул)
async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis
def init_redis_client():
    return aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )

# 🔸 Загрузка тикеров из базы
async def load_tickers(pg_pool) -> Dict[str, Dict[str, int]]:
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, precision_price FROM tickers WHERE status = 'enabled';"
        )
        result = {
            row["symbol"]: {
                "precision_price": row["precision_price"]
            } for row in rows
        }
        debug_log(f"🔹 Загружено тикеров: {json.dumps(result, indent=2)}")
        return result

# 🔸 Главная точка входа
async def main():
    logging.info("🚀 indicators_v2_main.py запущен.")

    pg_pool = await init_pg_pool()
    redis = init_redis_client()

    global tickers_storage
    tickers_storage = await load_tickers(pg_pool)
    logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")

    # Заглушка: основной цикл
    while True:
        await asyncio.sleep(60)

# ▶️ Запуск
if __name__ == "__main__":
    asyncio.run(main())
