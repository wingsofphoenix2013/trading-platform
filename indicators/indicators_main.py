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
import os
import json

from ema import process_ema
from smi import process_smi
from atr import process_atr
from rsi import process_rsi

# Подключение к PostgreSQL и Redis (через переменные окружения)
DATABASE_URL = os.getenv('DATABASE_URL')
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_INDICATOR_PREFIX = os.getenv('REDIS_INDICATOR_PREFIX', 'indicators')
REDIS_CHANNEL_INDICATORS = os.getenv('REDIS_CHANNEL_INDICATORS', None)

# Таймфреймы, по которым будут считаться индикаторы
TIMEFRAMES = ['M1', 'M5', 'M15']

# Глобальный словарь с точностями
precision_map = {}

# 1. Фоновое обновление списка тикеров
async def refresh_precision_map(pg_pool):
    global precision_map
    while True:
        try:
            async with pg_pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol, precision_price FROM tickers WHERE status = 'enabled'")
                precision_map = {r['symbol']: r['precision_price'] for r in rows}
                print(f"[TICKERS] Обновлено: {len(precision_map)} активных тикеров", flush=True)
        except Exception as e:
            print(f"[ERROR] Ошибка при обновлении тикеров: {e}", flush=True)
        await asyncio.sleep(60)


# 2. Обработка одного тикера по одному таймфрейму
async def process_ticker(tf, ticker, pg_pool, redis):
    symbol = ticker['symbol']
    precision = ticker['precision_price']
    print(f"[LOOP] Расчёт для {symbol} / {tf}", flush=True)
    try:
        await process_ema(pg_pool, redis, symbol, tf, precision)
        await process_smi(pg_pool, redis, symbol, tf, precision)
        await process_atr(pg_pool, redis, symbol, tf, precision)
        await process_rsi(pg_pool, redis, symbol, tf, precision)
    except Exception as e:
        print(f"[ERROR] Ошибка в расчёте индикаторов для {symbol}/{tf}: {e}", flush=True)


# 3. Главная точка входа
async def main():
    print("[INIT] Старт координатора индикаторов", flush=True)
    await asyncio.sleep(10)

    pg_pool = await asyncpg.create_pool(DATABASE_URL)
    redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )
    print("[Redis] Подключение к Redis установлено", flush=True)

    # Старт фоновой задачи обновления тикеров
    asyncio.create_task(refresh_precision_map(pg_pool))

    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_m5_complete")
    print("[Sub] Подписка на канал ohlcv_m5_complete активна", flush=True)

    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            data = json.loads(message['data'].decode())
            symbol = data['symbol']
            tf = 'M5'

            precision = precision_map.get(symbol)
            if precision is None:
                print(f"[SKIP] {symbol} отсутствует в списке активных тикеров", flush=True)
                continue

            ticker = {'symbol': symbol, 'precision_price': precision}
            await process_ticker(tf, ticker, pg_pool, redis)

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке события: {e}", flush=True)


if __name__ == '__main__':
    asyncio.run(main())
