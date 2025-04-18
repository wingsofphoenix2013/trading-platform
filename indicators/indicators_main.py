# Шаг 1. Инициализация: подключение к Redis, PostgreSQL и подписка на канал завершённых свечей

import asyncio
import os
import asyncpg
import redis.asyncio as redis
import json
import pandas as pd
import numpy as np
from datetime import datetime
from math import atan, degrees

REDIS_CHANNEL_IN = 'ohlcv_m5_complete'

print("🚀 INDICATORS WORKER STARTED", flush=True)

async def main():
    # Подключение к Redis
    print("[INIT] Connecting to Redis...", flush=True)
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD"),
            db=0,
            decode_responses=True,
            ssl=True
        )
        await redis_client.ping()
        print("[OK] Connected to Redis", flush=True)

        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_CHANNEL_IN)
        print(f"[INIT] Subscribed to Redis channel: {REDIS_CHANNEL_IN}", flush=True)

    except Exception as e:
        print(f"[ERROR] Redis connection or subscription failed: {e}", flush=True)
        return

    # Подключение к PostgreSQL
    print("[INIT] Connecting to PostgreSQL...", flush=True)
    try:
        pg_conn = await asyncpg.connect(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT", "5432"),
            database=os.getenv("PG_NAME")
        )
        print("[OK] Connected to PostgreSQL", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to connect PostgreSQL: {e}", flush=True)
        return

    # Шаг 2. Загрузка свечей, настроек и точности по каждому сообщению из Redis
    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            data = json.loads(message['data'])
            symbol = data.get("symbol")
            ts_str = data.get("timestamp")
            print(f"[REDIS] Получено сообщение: symbol={symbol}, timestamp={ts_str}", flush=True)

            # Загрузка последних 100 завершённых свечей
            query_candles = """
                SELECT open_time AS timestamp, open, high, low, close, volume
                FROM ohlcv_m5
                WHERE symbol = $1 AND complete = true
                ORDER BY open_time DESC
                LIMIT 100
            """
            rows = await pg_conn.fetch(query_candles, symbol)
            if not rows or len(rows) < 20:
                print(f"[SKIP] Недостаточно данных для {symbol} ({len(rows)} свечей)", flush=True)
                continue

            import pandas as pd
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp')
            print(f"[DATA] Загружено {len(df)} свечей для {symbol}", flush=True)

            # Загрузка параметров индикаторов
            query_settings = "SELECT indicator, param, value FROM indicator_settings"
            rows = await pg_conn.fetch(query_settings)
            settings = {}
            for row in rows:
                indicator = row['indicator']
                param = row['param']
                value = float(row['value'])
                if indicator not in settings:
                    settings[indicator] = {}
                settings[indicator][param] = value
            print(f"[DATA] Загружены параметры индикаторов: {settings}", flush=True)

            # Загрузка precision_price
            query_precision = "SELECT precision_price FROM tickers WHERE symbol = $1"
            precision_row = await pg_conn.fetchrow(query_precision, symbol)
            precision_digits = int(precision_row['precision_price']) if precision_row else 2
            print(f"[DATA] Точность цен для {symbol}: {precision_digits} знаков после запятой", flush=True)

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке сообщения: {e}", flush=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Остановлено вручную", flush=True)
