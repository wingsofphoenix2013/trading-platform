# aggregator.py
# Агрегация свечей M1 в M5, M15, M30, H1, H4 по сигналам из Redis

# 0. Импорты
import asyncio
import asyncpg
import redis.asyncio as aioredis
import json
from datetime import datetime, timedelta

# 1. Подписка на Redis и запуск агрегации
async def start_aggregator(redis, pg_pool):
    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_aggregate")
    print("[AGGREGATOR] Подписка на ohlcv_aggregate активна", flush=True)

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            symbol = data["symbol"]
            interval = data["interval"]
            until_str = data["until"]
            until_dt = datetime.fromisoformat(until_str)

            await aggregate_candles(pg_pool, symbol, interval, until_dt)

        except Exception as e:
            print(f"[ERROR] Ошибка в start_aggregator: {e}", flush=True)


# Заглушка под будущую реализацию
async def aggregate_candles(pg_pool, symbol, interval, until_time):
    pass
