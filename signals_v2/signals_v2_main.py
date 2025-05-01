import os
import asyncio
import logging
import redis.asyncio as redis
import asyncpg
from datetime import datetime

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

# 🔸 Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# 🔸 Обработка одного сигнала из потока
async def process_signal(entry_id, data):
    logging.info(f"📥 Получен сигнал из Redis Stream: {data}")
    # TODO: в будущем — парсинг, проверка, запись в БД и отправка в strategy_tasks

# 🔸 Цикл чтения сигналов из Redis Streams
async def listen_signals():
    logging.info("🚀 Signal Worker (v2) запущен. Ожидание сигналов...")
    group = "workers"
    consumer = f"consumer-{os.getpid()}"

    try:
        # 🔹 Создание группы (только один раз при первом запуске)
        await redis_client.xgroup_create("signals_stream", group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует. Продолжаем.")
        else:
            raise

    while True:
        result = await redis_client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={"signals_stream": ">"},
            count=10,
            block=500
        )

        if result:
            for stream_name, messages in result:
                for entry_id, data in messages:
                    await process_signal(entry_id, data)
                    await redis_client.xack("signals_stream", group, entry_id)

# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(listen_signals())