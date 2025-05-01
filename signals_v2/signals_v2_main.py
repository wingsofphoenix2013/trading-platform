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

# 🔸 Глобальный словарь тикеров: symbol → tradepermission
TICKERS = {}

# 🔸 Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# 🔸 Загрузка тикеров из БД (status = enabled)
async def load_tickers():
    global TICKERS
    try:
        conn = await get_db()
        rows = await conn.fetch("SELECT symbol, tradepermission FROM tickers WHERE status = 'enabled'")
        TICKERS = {row["symbol"]: row["tradepermission"] for row in rows}
        logging.info(f"✅ Загрузка тикеров: {len(TICKERS)} шт.")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке тикеров: {e}")
    finally:
        await conn.close()

# 🔸 Фоновое обновление тикеров каждые 5 минут
async def refresh_tickers_periodically():
    while True:
        await load_tickers()
        await asyncio.sleep(300)

# 🔸 Обработка одного сигнала из потока
async def process_signal(entry_id, data):
    logging.info(f"📥 Получен сигнал из Redis Stream: {data}")
    # TODO: парсинг, проверка message и symbol, сверка с TICKERS, запись в БД

# 🔸 Цикл чтения сигналов из Redis Stream
async def listen_signals():
    logging.info("🚀 Signal Worker (v2) запущен. Ожидание сигналов...")
    group = "workers"
    consumer = f"consumer-{os.getpid()}"

    try:
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

# 🔸 Главная точка запуска: загрузка тикеров + запуск слушателя сигналов
async def main():
    await load_tickers()
    asyncio.create_task(refresh_tickers_periodically())
    await listen_signals()

# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())