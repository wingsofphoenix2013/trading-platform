import os
import asyncio
import logging
import redis.asyncio as redis
import asyncpg
from decimal import Decimal

# 🔸 Настройка логирования
logging.basicConfig(level=logging.INFO)

# 🔸 Переменные окружения
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

# 🔸 Redis клиент
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# 🔸 Хранилища в памяти
open_positions = {}
tickers_storage = {}

# 🔸 Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# 🔸 Основной обработчик задач
async def handle_task(entry_id, data):
    logging.info(f"📥 Получена задача: {data}")
    # TODO: вызов стратегии через on_signal(data)

# 🔸 Слушаем Redis Stream
async def listen_strategy_tasks():
    group = "strategy-workers"
    consumer = f"worker-{os.getpid()}"

    try:
        await redis_client.xgroup_create("strategy_tasks", group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует.")
        else:
            raise

    while True:
        result = await redis_client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={"strategy_tasks": ">"},
            count=10,
            block=500
        )

        if result:
            for stream_name, messages in result:
                for entry_id, data in messages:
                    await handle_task(entry_id, data)
                    await redis_client.xack("strategy_tasks", group, entry_id)

# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")
    await listen_strategy_tasks()

if __name__ == "__main__":
    asyncio.run(main())