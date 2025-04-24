# aggregator.py
# Агрегация свечей M1 в старшие таймфреймы по сигналам из Redis

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

            await aggregate_candles(pg_pool, redis, symbol, interval, until_dt)

        except Exception as e:
            print(f"[ERROR] Ошибка в start_aggregator: {e}", flush=True)


# 2. Агрегация свечей M1 в OHLCV для указанного интервала
async def aggregate_candles(pg_pool, redis, symbol, interval, until_time):
    interval_map = {
        "m5": 5,
        "m15": 15,
        "m30": 30,
        "h1": 60,
        "h4": 240
    }

    if interval not in interval_map:
        print(f"[AGGREGATOR] Пропущен неизвестный интервал: {interval}", flush=True)
        return

    count = interval_map[interval]
    start_time = until_time - timedelta(minutes=count - 1)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, open, high, low, close, volume
            FROM ohlcv2_m1
            WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
            ORDER BY open_time ASC
            """,
            symbol,
            start_time,
            until_time
        )

        if len(rows) != count:
            print(f"[AGGREGATOR] Недостаточно данных для {symbol} {interval} @ {until_time}", flush=True)
            return

        open = rows[0]["open"]
        close = rows[-1]["close"]
        high = max(r["high"] for r in rows)
        low = min(r["low"] for r in rows)
        volume = sum(r["volume"] for r in rows)

        table_name = f"ohlcv2_{interval}"

        await conn.execute(
            f"""
            INSERT INTO {table_name} (symbol, open_time, open, high, low, close, volume, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'aggregated')
            ON CONFLICT DO NOTHING
            """,
            symbol,
            start_time,
            open,
            high,
            low,
            close,
            volume
        )

        print(f"[AGGREGATOR] Агрегация {interval} готова для {symbol} @ {until_time}", flush=True)

        # Публикация готовности интервала
        channel = f"ohlcv_{interval}_ready"
        message = {
            "action": "aggregate_ready",
            "symbol": symbol,
            "interval": interval,
            "open_time": start_time.isoformat()
        }
        try:
            await redis.publish(channel, json.dumps(message))
            print(f"[REDIS] Published to {channel}: {message}", flush=True)
        except Exception as e:
            print(f"[ERROR] Ошибка публикации в Redis: {e}", flush=True)
