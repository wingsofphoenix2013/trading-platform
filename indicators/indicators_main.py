# indicators/indicators_main.py — расчёт технических индикаторов (этап 2)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis
import json
import pandas as pd
from datetime import datetime

# === Конфигурация ===
REDIS_CHANNEL = 'ohlcv_m5_complete'

# === Асинхронный запуск подписки на Redis и подключение к PostgreSQL ===
async def main():
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
        await pubsub.subscribe(REDIS_CHANNEL)
        print(f"[INIT] Subscribed to Redis channel: {REDIS_CHANNEL}", flush=True)

    except Exception as e:
        print(f"[ERROR] Redis connection or subscription failed: {e}", flush=True)
        return

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

    # Слушаем Redis-канал и логируем поступающие сообщения
    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                symbol = data.get("symbol")
                ts_str = data.get("timestamp")
                print(f"[REDIS] Получено сообщение: symbol={symbol}, timestamp={ts_str}", flush=True)

                # === ЭТАП 2: Загрузка последних 100 свечей по тикеру ===
                query_candles = """
                    SELECT timestamp, open, high, low, close, volume
                    FROM ohlcv_m5
                    WHERE symbol = $1 AND complete = true
                    ORDER BY timestamp DESC
                    LIMIT 100
                """
                rows = await pg_conn.fetch(query_candles, symbol)
                if not rows or len(rows) < 20:
                    print(f"[SKIP] Недостаточно данных для {symbol} ({len(rows)} свечей)", flush=True)
                    continue

                df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df = df.sort_values('timestamp')  # от старых к новым

                print(f"[DATA] Загружено {len(df)} свечей для {symbol}", flush=True)

                # === Загрузка всех параметров из indicator_settings ===
                query_settings = """
                    SELECT indicator, param, value
                    FROM indicator_settings
                """
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

            except Exception as e:
                print(f"[ERROR] Ошибка при обработке сообщения: {e}", flush=True)

# === Точка входа ===
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Остановлено вручную", flush=True)