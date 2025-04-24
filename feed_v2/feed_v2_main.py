# feed_v2_main.py
# Координатор потоков: подключение к БД, Redis и запуск компонентов feed-системы

# 0. Импорты
import asyncio
import asyncpg
import redis.asyncio as aioredis
import os
from m1_handler import start_all_m1_streams

# 1. Инициализация переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


# 2. Основная точка входа
async def main():
    print("[INIT] feed_v2_main стартует", flush=True)
    await asyncio.sleep(2)

    pg_pool = None
    redis = None

    try:
        # 2.1 Подключение к PostgreSQL
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        print("[PG] Подключение к PostgreSQL установлено", flush=True)

        # 2.2 Подключение к Redis
        redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )
        await redis.ping()
        print("[Redis] Подключение к Redis установлено", flush=True)

        # 2.3 Запуск подписки на тикеры и WebSocket-потоков
        await start_all_m1_streams(redis, pg_pool)

    finally:
        if redis:
            await redis.aclose()
        if pg_pool:
            await pg_pool.close()
        print("[CLOSE] Соединения закрыты", flush=True)


# 3. Точка входа в асинхронный цикл
if __name__ == "__main__":
    asyncio.run(main())
