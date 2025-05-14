import os
import json
import asyncio
import logging
import redis.asyncio as redis
import asyncpg
from datetime import datetime

from debug_utils import debug_log
from signal_ema_cross import process_ema_cross_signal

# 🔸 Конфигурация логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# 🔸 Redis настройки из переменных окружения
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# 🔸 Инициализация Redis клиента
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# 🔸 In-memory хранилище значений индикаторов
signal_state_storage = {}

# 🔸 Диспетчер обработчиков по типу индикатора
INDICATOR_DISPATCH = {
    "EMA": process_ema_cross_signal,
    # другие индикаторы будут добавлены позже
}

# 🔸 Унифицированная публикация сигнала в Redis Stream
async def publish_to_signals_stream(symbol: str, message: str, time: str):
    sent_at = datetime.utcnow().isoformat()
    try:
        await redis_client.xadd("signals_stream", {
            "message": message,
            "symbol": symbol,
            "time": time,
            "sent_at": sent_at
        })
        logging.info(f"📤 Сигнал опубликован: {message} / {symbol}")
    except Exception as e:
        logging.error(f"Ошибка публикации сигнала: {e}")

# 🔸 Основной цикл прослушивания Redis Stream
async def listen_to_indicators(db_pool):
    group = "indicator_signal_workers"
    consumer = f"worker-{os.getpid()}"

    try:
        await redis_client.xgroup_create("indicators_ready_stream", group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует.")
        else:
            raise

    while True:
        result = await redis_client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={"indicators_ready_stream": ">"},
            count=10,
            block=1000
        )

        if not result:
            continue

        for stream_name, messages in result:
            for entry_id, data in messages:
                await handle_indicator_message(data, db_pool)
                await redis_client.xack("indicators_ready_stream", group, entry_id)

# 🔸 Обработка одного сообщения индикатора
async def handle_indicator_message(data: dict, db_pool):
    try:
        symbol = data["symbol"]
        timeframe = data["timeframe"]
        indicator = data["indicator"]
        params = json.loads(data["params"])
        calculated_at = data["calculated_at"]

        processor = INDICATOR_DISPATCH.get(indicator)
        if processor:
            await processor(
                symbol=symbol,
                timeframe=timeframe,
                params=params,
                ts=calculated_at,
                state=signal_state_storage,
                publish=publish_to_signals_stream,
                db_pool=db_pool
            )
        else:
            debug_log(f"Пропущен неподдерживаемый индикатор: {indicator}")

    except Exception as e:
        logging.error(f"Ошибка обработки сообщения: {e}")

# 🔸 Точка входа
async def main():
    logging.info("🚀 Indicator Signal Worker запущен")

    db_pool = await asyncpg.create_pool(dsn=os.getenv("DATABASE_URL"))
    await listen_to_indicators(db_pool)

if __name__ == "__main__":
    asyncio.run(main())