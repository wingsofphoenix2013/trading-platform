import asyncio
import os
import json
import logging
import asyncpg
import redis.asyncio as aioredis
import numpy as np
import pandas as pd
import pandas_ta as pta
import ta
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from typing import Dict, Any
# 🔸 Импорты файлов индикаторов
from ema import process_ema
# 🔸 Конфигурация логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# 🔸 Флаг режима отладки
DEBUG_MODE = False  # Включай True при разработке

def debug_log(message: str):
    if DEBUG_MODE:
        logging.info(message)

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# 🔸 In-memory хранилища
tickers_storage: Dict[str, Dict[str, int]] = {}
ohlcv_cache: Dict[str, Dict[str, Any]] = {}
indicator_configs: Dict[int, Dict[str, Any]] = {}

# 🔸 Подключение к PostgreSQL (асинхронный пул)
async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis
def init_redis_client():
    return aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )

# 🔸 Загрузка тикеров из базы
async def load_tickers(pg_pool) -> Dict[str, Dict[str, int]]:
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, precision_price FROM tickers WHERE status = 'enabled';"
        )
        result = {
            row["symbol"]: {
                "precision_price": row["precision_price"]
            } for row in rows
        }
        debug_log(f"🔹 Загружено тикеров: {len(result)}")
        return result
# 🔸 Загрузка конфигураций расчётных индикаторов
async def load_indicator_config(pg_pool) -> Dict[int, Dict[str, Any]]:
    async with pg_pool.acquire() as conn:
        instances = await conn.fetch(
            "SELECT id, indicator, timeframe, stream_publish FROM indicator_instances_v2 WHERE enabled = true"
        )
        instance_ids = [row["id"] for row in instances]
        if not instance_ids:
            return {}

        params = await conn.fetch(
            "SELECT instance_id, param, value FROM indicator_parameters_v2 WHERE instance_id = ANY($1)",
            instance_ids
        )

    # Построение in-memory конфигурации
    config = {}
    for row in instances:
        config[row["id"]] = {
            "indicator": row["indicator"],
            "timeframe": row["timeframe"],
            "stream_publish": row["stream_publish"],
            "params": {}
        }

    for param in params:
        config[param["instance_id"]]["params"][param["param"]] = param["value"]

    debug_log(f"📦 Загружено конфигураций индикаторов: {len(config)}")
    return config
# 🔸 Подписка на события Pub/Sub от агрегаторов
async def subscribe_to_ohlcv(redis, pg_pool):
    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_m1_ready", "ohlcv_m5_ready", "ohlcv_m15_ready")
    logging.info("📡 Подписка на каналы ohlcv_m1_ready, ohlcv_m5_ready, ohlcv_m15_ready активна.")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"].decode())
            channel = message["channel"].decode()

            if channel == "ohlcv_m1_ready" and data.get("action") == "m1_ready":
                symbol = data["symbol"]
                tf = "M1"
                open_time = data["open_time"]

            elif channel in ("ohlcv_m5_ready", "ohlcv_m15_ready") and data.get("action") == "aggregate_ready":
                symbol = data["symbol"]
                tf = data["interval"].upper()
                open_time = data["open_time"]

            else:
                continue  # неизвестный формат — пропускаем

            debug_log(f"📥 Получено событие: {symbol} / {tf} / {open_time}")

            candles = await get_latest_ohlcv(symbol, tf, open_time, pg_pool)
            if candles.empty:
                logging.warning(f"⚠️ Расчёт прерван: нет свечей для {symbol} / {tf} / {open_time}")
                continue

            # 🔹 Найти EMA-инстансы для данного timeframe
            ema_instances = [
                (instance_id, cfg)
                for instance_id, cfg in indicator_configs.items()
                if cfg["indicator"].upper() == "EMA" and cfg["timeframe"].upper() == tf
            ]

            # 🔹 Выполнить расчёт EMA
            for instance_id, cfg in ema_instances:
                await process_ema(
                    instance_id=instance_id,
                    symbol=symbol,
                    tf=tf,
                    open_time=open_time,
                    params=cfg["params"],
                    candles=candles,
                    redis=redis,
                    db=pg_pool,
                    precision_price=tickers_storage[symbol]["precision_price"]
                )

        except Exception as e:
            logging.error(f"❌ Ошибка при обработке события PubSub: {e}")
# 🔸 Получение и кэширование свечей
async def get_latest_ohlcv(symbol: str, tf: str, open_time: str, pg_pool) -> pd.DataFrame:
    cache_key = f"{symbol}:{tf}"

    try:
        incoming_time = datetime.fromisoformat(open_time)
    except Exception:
        logging.error(f"❌ Невалидный формат open_time: {open_time}")
        return pd.DataFrame()

    # Проверка кэша
    cached = ohlcv_cache.get(cache_key)
    if cached:
        cached_time = cached["open_time"]
        if incoming_time < cached_time:
            logging.warning(f"⚠️ Устаревшее событие: {symbol} / {tf} / {open_time}")
            return cached["candles"]
        if incoming_time == cached_time:
            debug_log(f"🧠 Используем кэш для {symbol} / {tf} / {open_time}")
            return cached["candles"]

    # Загрузка новых свечей из базы
    table_name = f"ohlcv2_{tf.lower()}"
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT open_time, high, low, close
                FROM {table_name}
                WHERE symbol = $1
                ORDER BY open_time DESC
                LIMIT 250
                """,
                symbol
            )
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке свечей для {symbol} / {tf}: {e}")
        return pd.DataFrame()

    if not rows or len(rows) < 10:
        logging.warning(f"⚠️ Недостаточно данных OHLCV для {symbol} / {tf}")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["open_time", "high", "low", "close"])
    df = df[::-1]  # Сортировка по времени (ASC)

    # Обновление кэша
    ohlcv_cache[cache_key] = {
        "open_time": incoming_time,
        "candles": df
    }

    debug_log(f"📊 Загружены {len(df)} свечей для {symbol} / {tf} / {open_time}")
    return df
# 🔄 Периодическое обновление тикеров и конфигураций
async def refresh_all_periodically(pg_pool):
    while True:
        await asyncio.sleep(300)  # каждые 5 минут
        try:
            global tickers_storage
            global indicator_configs
            tickers_storage = await load_tickers(pg_pool)
            indicator_configs = await load_indicator_config(pg_pool)
            logging.info("🔄 Обновлены тикеры и конфигурации индикаторов")
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении тикеров/конфигураций: {e}")                
# 🔸 Главная точка входа
async def main():
    logging.info("🚀 indicators_v2_main.py запущен.")

    pg_pool = await init_pg_pool()
    redis = init_redis_client()

    global tickers_storage
    tickers_storage = await load_tickers(pg_pool)
    logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")
    
    global indicator_configs
    indicator_configs = await load_indicator_config(pg_pool)
    logging.info(f"📥 Конфигураций расчёта: {len(indicator_configs)}")
    
    asyncio.create_task(subscribe_to_ohlcv(redis, pg_pool))
    asyncio.create_task(refresh_all_periodically(pg_pool))

    # Заглушка: основной цикл
    while True:
        await asyncio.sleep(60)

# ▶️ Запуск
if __name__ == "__main__":
    asyncio.run(main())
