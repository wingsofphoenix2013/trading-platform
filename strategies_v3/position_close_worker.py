import asyncio
import logging
import json
import os
import redis.asyncio as redis
from redis.exceptions import ResponseError
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from debug_utils import debug_log
import asyncpg


# 🔧 Переменные окружения
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

# 🔧 Redis клиент
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# 🔧 Группа и поток
STREAM_NAME = "position:close"
GROUP_NAME = "position_closer"
CONSUMER_NAME = "position_closer_worker"

# 🔧 Инициализация consumer group
async def init_stream():
    try:
        await redis_client.xgroup_create(name=STREAM_NAME, groupname=GROUP_NAME, id="0", mkstream=True)
        logging.info("✅ Группа position_closer создана")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует")
        else:
            raise

# 🔁 Основной цикл обработки
async def listen_close_tasks(db_pool):
    while True:
        try:
            entries = await redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=10,
                block=1000
            )

            for stream, messages in entries:
                for msg_id, data in messages:
                    logging.info(f"📥 Получена задача на закрытие позиции: {data}")
                    # Пока просто подтверждаем
                    await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)

        except Exception as e:
            logging.error(f"❌ Ошибка при чтении потока закрытия: {e}")
            await asyncio.sleep(1)

# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Запуск position_close_worker")
    await init_stream()
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    await listen_close_tasks(db_pool)

if __name__ == "__main__":
    asyncio.run(main())