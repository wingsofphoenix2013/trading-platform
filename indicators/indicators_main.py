# indicators_main.py
# Координатор расчёта всех индикаторов по таймфреймам и тикерам

# 0. Импорты
import asyncio
import asyncpg
import redis.asyncio as aioredis

# Вспомогательные библиотеки для всех индикаторов
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal
import math

# Импорты индикаторов (расширяются по мере добавления)
# ---------------------------
from ema import process_ema  # EMA 50/100/200
# from atr import process_atr
# from lr import process_lr
# ---------------------------

# Подключение к PostgreSQL и Redis
DATABASE_URL = 'postgresql://user:password@localhost:5432/dbname'
REDIS_URL = 'redis://localhost'

# Таймфреймы, по которым будут считаться индикаторы
TIMEFRAMES = ['M1', 'M5', 'M15']


# 1. Главная точка входа
async def main():
    print("[INIT] Старт координатора индикаторов", flush=True)

    # Подключение к PostgreSQL
    pg_pool = await asyncpg.create_pool(DATABASE_URL)
    print("[DB] Подключение к PostgreSQL установлено", flush=True)

    # Подключение к Redis
    redis = aioredis.from_url(REDIS_URL)
    print("[Redis] Подключение к Redis установлено", flush=True)

    async with pg_pool.acquire() as conn:
        # Получаем активные тикеры
        rows = await conn.fetch("SELECT symbol, precision_price FROM tickers WHERE enabled = true")
        tickers = [dict(r) for r in rows]

    print(f"[INFO] Найдено активных тикеров: {len(tickers)}", flush=True)

    await process_all(pg_pool, redis, tickers)

    print("[DONE] Завершено", flush=True)


# 2. Обработка всех тикеров по всем таймфреймам
async def process_all(pg_pool, redis, tickers):
    for tf in TIMEFRAMES:
        for ticker in tickers:
            await process_ticker(tf, ticker, pg_pool, redis)


# 3. Обработка одного тикера по одному таймфрейму
async def process_ticker(tf, ticker, pg_pool, redis):
    symbol = ticker['symbol']
    precision = ticker['precision_price']
    print(f"[LOOP] Расчёт для {symbol} / {tf}", flush=True)

    # Вызовы всех индикаторов для данного тикера и таймфрейма
    # ---------------------------
    try:
        await process_ema(pg_pool, redis, symbol, tf, precision)
    except Exception as e:
        print(f"[ERROR] Ошибка в EMA для {symbol}/{tf}: {e}", flush=True)
    # ---------------------------


if __name__ == '__main__':
    asyncio.run(main())
