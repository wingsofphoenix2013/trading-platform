# 🔸 Основной воркер стратегий v3

import os
import asyncio
import logging
import redis
import json
from strategy_1 import Strategy1
from strategies_v3_interface import StrategyInterface

# 🔸 Конфигурация логирования
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

# 🔸 Хранилище стратегий (регистрируются вручную)
strategies = {
    "strategy_1": Strategy1(),
}

# 🔸 Обработчик одной задачи
async def handle_task(task_data: dict):
    strategy_name = task_data.get("strategy")
    strategy = strategies.get(strategy_name)

    if not strategy:
        logging.warning(f"⚠️ Стратегия не найдена: {strategy_name}")
        return

    interface = StrategyInterface(redis_client=redis_client, database_url=DATABASE_URL)

    try:
        await strategy.on_signal(task_data, interface)
    except Exception as e:
        logging.error(f"❌ Ошибка при вызове стратегии {strategy_name}: {e}")

# 🔸 Слушатель задач из Redis Stream
async def listen_strategy_tasks():
    group_name = "strategy_group"
    consumer_name = "strategy_worker"
    stream_name = "strategy_tasks"

    try:
        redis_client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        logging.info("✅ Группа создана.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует.")
        else:
            raise

    while True:
        try:
            entries = redis_client.xreadgroup(group_name, consumer_name, {stream_name: ">"}, count=10, block=5000)
            for stream, messages in entries:
                for msg_id, msg_data in messages:
                    logging.info(f"📥 Получена задача: {msg_data}")
                    await handle_task(msg_data)
                    redis_client.xack(stream_name, group_name, msg_id)
        except Exception as e:
            logging.error(f"❌ Ошибка при чтении из Redis Stream: {e}")
            await asyncio.sleep(1)

# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")
    await listen_strategy_tasks()

if __name__ == "__main__":
    asyncio.run(main())