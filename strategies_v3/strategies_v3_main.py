# 🔸 Основной воркер стратегий v3

import os
import asyncio
import logging
import redis.asyncio as redis
from redis.exceptions import ResponseError
import json
import asyncpg
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

# 🔸 Хранилища в памяти
tickers_storage = {}
open_positions = {}
latest_prices = {}
strategies_cache = {}
strategy_allowed_tickers = {}
open_positions = {}

# 🔸 Хранилище стратегий (регистрируются вручную)
strategies = {
    "strategy_1": Strategy1(),
}
# 🔸 Загрузка тикеров из базы
import asyncpg  # добавить в начало файла, если ещё нет

async def load_tickers():
    global tickers_storage

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("""
            SELECT symbol, precision_price, precision_qty, min_qty,
                   status, tradepermission, is_active
            FROM tickers
            WHERE status = 'enabled'
        """)
        await conn.close()

        tickers_storage = {
            row["symbol"]: {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"],
                "min_qty": float(row["min_qty"]),
                "status": row["status"],
                "tradepermission": row["tradepermission"],
                "is_active": row["is_active"]
            }
            for row in rows
        }

        logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке тикеров: {e}")
# 🔸 Периодическое обновление тикеров
async def refresh_tickers_periodically():
    while True:
        logging.info("🔄 Обновление тикеров...")
        await load_tickers()
        await asyncio.sleep(60)
# 🔸 Загрузка разрешённых тикеров по стратегиям
async def load_strategy_tickers():
    global strategy_allowed_tickers

    try:
        conn = await asyncpg.connect(DATABASE_URL)

        result = {}

        for strategy_id, strategy in strategies_cache.items():
            use_all = strategy.get("use_all_tickers", False)

            if use_all:
                # Все тикеры с разрешением (без учета is_active)
                allowed = {
                    symbol for symbol, t in tickers_storage.items()
                    if t["status"] == "enabled" and
                       t["tradepermission"] == "enabled"
                }
            else:
                rows = await conn.fetch("""
                    SELECT symbol
                    FROM strategy_tickers_v2
                    WHERE strategy_id = $1
                """, strategy_id)
                allowed = {row["symbol"] for row in rows}

            result[strategy_id] = allowed

        await conn.close()
        strategy_allowed_tickers = result

        total = sum(len(tickers) for tickers in result.values())
        logging.info(f"✅ Загружено разрешённых тикеров: {total} (для {len(result)} стратегий)")

    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке strategy_tickers: {e}")
# 🔸 Периодическое обновление стратегий и разрешённых тикеров
async def refresh_strategies_and_tickers_periodically():
    while True:
        logging.info("🔄 Обновление стратегий и тикеров по стратегиям...")
        await load_strategies()
        await load_strategy_tickers()
        await asyncio.sleep(60)                       
# 🔸 Обработчик одной задачи
async def handle_task(task_data: dict):
    strategy_name = task_data.get("strategy")
    strategy = strategies.get(strategy_name)

    if not strategy:
        logging.warning(f"⚠️ Стратегия не найдена: {strategy_name}")
        return

    interface = StrategyInterface(
        redis_client=redis_client,
        database_url=DATABASE_URL,
        strategies_cache=strategies_cache,
        strategy_allowed_tickers=strategy_allowed_tickers,
        open_positions=open_positions
    )

    # 🔹 Выполнение базовых проверок
    ok, note = await interface.run_basic_checks(task_data)
    logging.info(f"✅ Проверка: {ok}, Причина: {note}")

    if not ok:
        strategy_id = await interface.get_strategy_id_by_name(strategy_name)
        log_id = int(task_data.get("log_id"))

        await interface.log_strategy_action(
            strategy_id=strategy_id,
            log_id=log_id,
            status="ignored_by_check",
            note=note
        )
        return

    # 🔹 Вызов стратегии
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
        await redis_client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        logging.info("✅ Группа создана.")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа уже существует.")
        else:
            raise

    while True:
        try:
            entries = await redis_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=10,
                block=500
            )
            for stream, messages in entries:
                for msg_id, msg_data in messages:
                    logging.info(f"📥 Получена задача: {msg_data}")
                    await handle_task(msg_data)
                    await redis_client.xack(stream_name, group_name, msg_id)
        except Exception as e:
            logging.error(f"❌ Ошибка при чтении из Redis Stream: {e}")
            await asyncio.sleep(1)
# 🔸 Загрузка стратегий из базы
async def load_strategies():
    global strategies_cache

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("""
            SELECT *
            FROM strategies_v2
            WHERE enabled = true AND archived = false
        """)
        await conn.close()

        strategies_cache = {
            row["id"]: dict(row) for row in rows
        }

        logging.info(f"✅ Загружено стратегий: {len(strategies_cache)}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке стратегий: {e}")
# 🔸 Загрузка открытых позиций из базы
async def load_open_positions():
    global open_positions

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("""
            SELECT *
            FROM positions_v2
            WHERE status = 'open'
        """)
        await conn.close()

        open_positions = {
            row["id"]: dict(row) for row in rows
        }

        logging.info(f"✅ Загружено открытых позиций: {len(open_positions)}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке открытых позиций: {e}")  
                          
# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")
    await load_tickers()
    await load_strategies()
    await load_strategy_tickers()
    await load_open_positions()
    asyncio.create_task(refresh_tickers_periodically())
    asyncio.create_task(refresh_strategies_and_tickers_periodically())
    await listen_strategy_tasks()
    
if __name__ == "__main__":
    asyncio.run(main())