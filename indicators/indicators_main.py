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

# Импорты индикаторов (расширяются по мере добавления)
# ---------------------------
from ema import process_ema  # EMA 50/100/200
# from atr import process_atr
# from lr import process_lr
# ---------------------------

# Подключение к PostgreSQL и Redis (через переменные окружения)
DATABASE_URL = os.getenv('DATABASE_URL')

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_INDICATOR_PREFIX = os.getenv('REDIS_INDICATOR_PREFIX', 'indicators')
REDIS_CHANNEL_INDICATORS = os.getenv('REDIS_CHANNEL_INDICATORS', None)

# 1. Главная точка входа
async def main():
    print("[INIT] Старт координатора индикаторов", flush=True)

    # Подключение к PostgreSQL
    pg_pool = await asyncpg.create_pool(DATABASE_URL)
    print("[DB] Подключение к PostgreSQL установлено", flush=True)

    # Подключение к Redis
    redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )
    print("[Redis] Подключение к Redis установлено", flush=True)

    # Подписка на канал публикации свечей M5
    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_m5_complete")
    print("[Sub] Подписка на канал ohlcv_m5_complete активна", flush=True)

    # Получаем активные тикеры и сохраняем precision в словарь
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, precision_price FROM tickers WHERE status = 'enabled' ORDER BY symbol")
        precision_map = {r['symbol']: r['precision_price'] for r in rows}

    # Цикл обработки сообщений
    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            data = json.loads(message['data'].decode())
            symbol = data['symbol']
            tf = 'M5'

            precision = precision_map.get(symbol)
            if precision is None:
                print(f"[WARN] Неизвестный символ {symbol} — пропущен", flush=True)
                continue

            print(f"[TRIGGER] Получено событие окончания M5 для {symbol}", flush=True)

            # Вызовы всех индикаторов для данного тикера и таймфрейма
            # ---------------------------
            await asyncio.sleep(10)  # Ждём, чтобы свеча точно успела записаться в базу
            try:
                await process_ema(pg_pool, redis, symbol, tf, precision)
            except Exception as e:
                print(f"[ERROR] Ошибка в EMA для {symbol}/{tf}: {e}", flush=True)
            # ---------------------------

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке события: {e}", flush=True)


if __name__ == '__main__':
    asyncio.run(main())
