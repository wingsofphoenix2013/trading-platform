# indicators/indicators_main.py

# Импорты
import asyncio
import asyncpg
import redis.asyncio as redis
import json
import os
from datetime import datetime

# --------- Конфигурация из переменных окружения ---------
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_NAME = os.getenv("PG_NAME")
POSTGRES_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_NAME}"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_DB = 0  # фиксированно
REDIS_CHANNEL = 'ohlcv_m5_complete'

# --------- Асинхронный запуск подписки на Redis ---------
async def main():
    # Подключение к Redis
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)
    print(f"[INIT] Subscribed to Redis channel: {REDIS_CHANNEL}", flush=True)

    # Подключение к PostgreSQL
    try:
        pg_conn = await asyncpg.connect(POSTGRES_DSN)
        print("[INIT] Connected to PostgreSQL", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to connect PostgreSQL: {e}", flush=True)
        return

    # Слушаем Redis-канал и логируем сообщения
    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                symbol = data.get("symbol")
                ts = data.get("timestamp")
                print(f"[REDIS] Получено сообщение: symbol={symbol}, timestamp={ts}", flush=True)
            except Exception as e:
                print(f"[ERROR] Ошибка при обработке сообщения: {e}", flush=True)

# --------- Точка входа ---------
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Остановлено вручную", flush=True)