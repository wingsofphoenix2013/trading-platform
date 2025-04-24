# feed_v2_main.py
# Координатор потоков: подключение к БД, Redis и запуск компонентов feed-системы

# 0. Импорты
import asyncio
import asyncpg
import redis.asyncio as aioredis
import os

# 1. Инициализация переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


# 2. Основная точка входа
async def main():
    print("[INIT] feed_v2_main стартует", flush=True)
    await asyncio.sleep(2)  # небольшой стартовый лаг

    # 2.1 Подключение к PostgreSQL
    try:
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        print("[PG] Подключение к PostgreSQL установлено", flush=True)
    except Exception as e:
        print(f"[ERROR] Ошибка подключения к PostgreSQL: {e}", flush=True)
        return

    # 2.2 Подключение к Redis
    try:
        redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )
        await redis.ping()
        print("[Redis] Подключение к Redis установлено", flush=True)
    except Exception as e:
        print(f"[ERROR] Ошибка подключения к Redis: {e}", flush=True)
        return

    # (на этом этапе никаких компонентов ещё не запускается)


# 3. Точка входа в асинхронный цикл
if __name__ == "__main__":
    asyncio.run(main())
