# 🔸 Основной воркер стратегий v3

import os
import asyncio
import logging
import redis.asyncio as redis
from redis.exceptions import ResponseError
import json
import asyncpg
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from debug_utils import debug_log
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
targets_by_position = {}
latest_prices = {}
strategies_cache = {}
strategy_allowed_tickers = {}

# 🔸 Хранилище стратегий (регистрируются вручную)
strategies = {
    "strategy_1": Strategy1(),
}
# 🔸 Загрузка тикеров из базы
async def load_tickers(db_pool):
    global tickers_storage

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT symbol, precision_price, precision_qty, min_qty,
                       status, tradepermission, is_active
                FROM tickers
                WHERE status = 'enabled'
            """)

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

        debug_log(f"✅ Загружено тикеров: {len(tickers_storage)}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке тикеров: {e}")
# 🔸 Загрузка разрешённых тикеров по стратегиям
async def load_strategy_tickers(db_pool):
    global strategy_allowed_tickers

    try:
        async with db_pool.acquire() as conn:
            result = {}

            for strategy_id, strategy in strategies_cache.items():
                use_all = strategy.get("use_all_tickers", False)

                if use_all:
                    # Все тикеры с разрешением
                    allowed = {
                        symbol for symbol, t in tickers_storage.items()
                        if t["status"] == "enabled" and t["tradepermission"] == "enabled"
                    }
                else:
                    rows = await conn.fetch("""
                        SELECT t.symbol
                        FROM strategy_tickers_v2 st
                        JOIN tickers t ON st.ticker_id = t.id
                        WHERE st.strategy_id = $1 AND st.enabled = true
                    """, strategy_id)
                    allowed = {row["symbol"] for row in rows}

                result[strategy_id] = allowed

        strategy_allowed_tickers = result
        total = sum(len(tickers) for tickers in result.values())
        debug_log(f"✅ Загружено разрешённых тикеров: {total} (для {len(result)} стратегий)")

    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке strategy_tickers: {e}")
# 🔸 Периодическое обновление всех данных (тикеры, стратегии, разрешения, позиции)
async def refresh_all_periodically(db_pool):
    while True:
        debug_log("🔄 Обновление тикеров, стратегий и позиций...")
        await load_tickers(db_pool)
        await load_strategies(db_pool)
        await load_strategy_tickers(db_pool)
        await load_open_positions(db_pool)
        await load_position_targets(db_pool)
        await asyncio.sleep(60)
# 🔸 Фоновая задача: обновление цен из Redis (ключи вида price:<symbol>)
async def monitor_prices():
    while True:
        try:
            keys = await redis_client.keys("price:*")
            if keys:
                values = await redis_client.mget(keys)
                for key, value in zip(keys, values):
                    if value is None:
                        continue
                    symbol = key.split("price:")[1]
                    try:
                        price = Decimal(value)
                        precision = tickers_storage.get(symbol, {}).get("precision_price", 8)
                        rounded = price.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)
                        latest_prices[symbol] = rounded
                    except Exception as e:
                        logging.warning(f"⚠️ Ошибка обработки цены {key}: {value} — {e}")
        except Exception as e:
            logging.error(f"❌ Ошибка при чтении цен из Redis: {e}")
        await asyncio.sleep(1)                           
# 🔸 Обработчик одной задачи
async def handle_task(task_data: dict, db_pool):
    strategy_name = task_data.get("strategy")
    strategy = strategies.get(strategy_name)

    if not strategy:
        logging.warning(f"⚠️ Стратегия не найдена: {strategy_name}")
        return

    interface = StrategyInterface(
        redis_client=redis_client,
        db_pool=db_pool,
        strategies_cache=strategies_cache,
        strategy_allowed_tickers=strategy_allowed_tickers,
        open_positions=open_positions,
        tickers_storage=tickers_storage,
        latest_prices=latest_prices,
        targets_by_position=targets_by_position
    )

    # 🔹 Выполнение базовых проверок
    ok, note = await interface.run_basic_checks(task_data)
    debug_log(f"✅ Проверка: {ok}, Причина: {note}")

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

    # 🔹 Вызов стратегии с ограничением времени
    try:
        await asyncio.wait_for(
            strategy.on_signal(task_data, interface),
            timeout=10  # ← ограничение в 10 секунд
        )
    except asyncio.TimeoutError:
        logging.error(f"⏱️ Время выполнения стратегии '{strategy_name}' превышено (таймаут 10 сек)")
    except Exception as e:
        logging.error(f"❌ Ошибка при вызове стратегии {strategy_name}: {e}")
        
# 🔸 Слушатель задач из Redis Stream
async def listen_strategy_tasks(db_pool):
    group_name = "strategy_group"
    consumer_name = "strategy_worker"
    stream_name = "strategy_tasks"

    try:
        await redis_client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        debug_log("✅ Группа создана.")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            debug_log("ℹ️ Группа уже существует.")
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
                    debug_log(f"📥 Получена задача: {msg_data}")

                    try:
                        await handle_task(msg_data, db_pool)
                    except Exception as e:
                        logging.error(f"❌ Ошибка при обработке задачи: {e}")
                    finally:
                        await redis_client.xack(stream_name, group_name, msg_id)

        except Exception as e:
            logging.error(f"❌ Ошибка при чтении из Redis Stream: {e}")
            await asyncio.sleep(1)
# 🔸 Загрузка стратегий из базы
async def load_strategies(db_pool):
    global strategies_cache

    try:
        async with db_pool.acquire() as conn:
            # Загружаем стратегии
            rows = await conn.fetch("""
                SELECT *
                FROM strategies_v2
                WHERE enabled = true AND archived = false
            """)

            # Загружаем TP-уровни всех стратегий
            tp_levels = await conn.fetch("""
                SELECT *
                FROM strategy_tp_levels_v2
            """)

            # Группируем TP-уровни по strategy_id
            tp_levels_by_strategy = {}
            for row in tp_levels:
                sid = row["strategy_id"]
                tp_levels_by_strategy.setdefault(sid, []).append(dict(row))

            # Загружаем SL-поведение после TP
            tp_sl_rules = await conn.fetch("""
                SELECT *
                FROM strategy_tp_sl_v2
            """)

            # Группируем SL-настройки по strategy_id
            tp_sl_by_strategy = {}
            for row in tp_sl_rules:
                sid = row["strategy_id"]
                tp_sl_by_strategy.setdefault(sid, []).append(dict(row))

        # Формируем strategies_cache
        strategies_cache = {}
        for row in rows:
            sid = row["id"]
            strategy_dict = dict(row)
            strategy_dict["tp_levels"] = tp_levels_by_strategy.get(sid, [])
            strategy_dict["tp_sl_rules"] = tp_sl_by_strategy.get(sid, [])
            strategies_cache[sid] = strategy_dict

        debug_log(f"✅ Загружено стратегий: {len(strategies_cache)}")

    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке стратегий: {e}")
# 🔸 Загрузка открытых позиций из базы
async def load_open_positions(db_pool):
    global open_positions

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT *
                FROM positions_v2
                WHERE status = 'open'
            """)

        open_positions = {
            row["id"]: dict(row) for row in rows
        }

        debug_log(f"✅ Загружено открытых позиций: {len(open_positions)}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке открытых позиций: {e}")
# 🔸 Загрузка целей по позициям из базы
async def load_position_targets(db_pool):
    global targets_by_position

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT *
                FROM position_targets_v2
            """)

        # Группируем по position_id
        grouped = {}
        for row in rows:
            pid = row["position_id"]
            grouped.setdefault(pid, []).append(dict(row))

        targets_by_position = grouped

        total = sum(len(t) for t in grouped.values())
        logging.info(f"✅ Загружено целей: {total} для {len(targets_by_position)} позиций")

    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке целей позиции: {e}")
# 🔸 Мониторинг открытых позиций на достижение TP/SL
async def follow_positions():
    for position_id, pos in open_positions.items():
        symbol = pos["symbol"]
        direction = pos["direction"]

        latest_price = latest_prices.get(symbol)
        if latest_price is None:
            logging.warning(f"⚠️ Нет цены для {symbol}, позиция ID={position_id}")
            continue

        targets = targets_by_position.get(position_id, [])
        if not targets:
            logging.warning(f"⚠️ Позиция ID={position_id} не имеет целей")
            continue

        # 🔹 Текущая цена
        debug_log(f"📡 Позиция ID={position_id}, {symbol}, {direction} — текущая цена: {latest_price}")

        # 🔹 TP-контроль
        tp_levels = [
            t for t in targets
            if t["type"] == "tp" and not t["hit"] and not t["canceled"]
        ]
        tp_levels.sort(key=lambda x: x["level"])

        next_tp = None
        for tp in tp_levels:
            lvl = tp["level"]
            blockers = [
                b for b in tp_levels
                if b["level"] < lvl and b["tp_trigger_type"] == "signal" and not b["hit"]
            ]
            if blockers:
                continue
            if tp["tp_trigger_type"] != "price":
                continue
            next_tp = tp
            break

        if next_tp:
            tp_price = next_tp["price"]
            level = next_tp["level"]
            if direction == "long" and latest_price >= tp_price:
                debug_log(f"💡 Цена достигла TP уровня #{level} для позиции ID={position_id} — {latest_price} ≥ {tp_price}")
            elif direction == "short" and latest_price <= tp_price:
                debug_log(f"💡 Цена достигла TP уровня #{level} для позиции ID={position_id} — {latest_price} ≤ {tp_price}")

        # 🔹 SL-контроль
        sl = next((t for t in targets if t["type"] == "sl" and not t["hit"] and not t["canceled"]), None)
        if sl:
            sl_price = sl["price"]
            if direction == "long" and latest_price <= sl_price:
                debug_log(f"⚠️ Цена достигла SL для позиции ID={position_id} — {latest_price} ≤ {sl_price}")
            elif direction == "short" and latest_price >= sl_price:
                debug_log(f"⚠️ Цена достигла SL для позиции ID={position_id} — {latest_price} ≥ {sl_price}")
# 🔸 Цикл мониторинга открытых позиций
async def follow_positions_loop():
    while True:
        await follow_positions()
        await asyncio.sleep(1)
# 🔸 Обработка задач на закрытие позиции
async def position_close_loop(db_pool):
    stream_name = "position:close"
    group_name = "position_closer"
    consumer_name = "position_closer_worker"

    try:
        await redis_client.xgroup_create(name=stream_name, groupname=group_name, id="0", mkstream=True)
        logging.info("✅ Группа position_closer создана")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info("ℹ️ Группа position_closer уже существует")
        else:
            raise

    while True:
        try:
            entries = await redis_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=10,
                block=1000
            )

            for stream, messages in entries:
                for msg_id, data in messages:
                    logging.info(f"📥 Получена задача на закрытие позиции: {data}")
                try:
                    position_id = int(data["position_id"])
                    target_id = int(data["target_id"])
                except (KeyError, ValueError):
                    logging.error("❌ Некорректные данные: отсутствует position_id или target_id")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                position = open_positions.get(position_id)
                if not position:
                    logging.warning(f"⚠️ Позиция {position_id} не найдена в памяти")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                targets = targets_by_position.get(position_id, [])
                target = next((t for t in targets if t.get("id") == target_id), None)

                if not target:
                    logging.warning(f"⚠️ Цель {target_id} не найдена в памяти позиции {position_id}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            UPDATE position_targets_v2
                            SET hit = true, hit_at = NOW()
                            WHERE id = $1
                        """, target_id)
                except Exception as e:
                    logging.error(f"❌ Ошибка при обновлении цели {target_id} в БД: {e}")
                    await redis_client.xack(stream_name, group_name, msg_id)
                    continue

                # Удаление цели из памяти
                targets_by_position[position_id] = [
                    t for t in targets if t.get("id") != target_id
                ]
                    
                    await redis_client.xack(stream_name, group_name, msg_id)

        except Exception as e:
            logging.error(f"❌ Ошибка в position_close_loop: {e}")
            await asyncio.sleep(1)                                                                  
# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")

    # 🔹 Создание пула PostgreSQL
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    logging.info("✅ Пул подключений к PostgreSQL создан")

    # 🔹 Загрузка всех in-memory хранилищ
    await load_tickers(db_pool)
    await load_strategies(db_pool)
    await load_strategy_tickers(db_pool)
    await load_open_positions(db_pool)
    await load_position_targets(db_pool)

    # 🔹 Фоновые обновления (можно оставить отключёнными)
    asyncio.create_task(refresh_all_periodically(db_pool))
    asyncio.create_task(monitor_prices())
    asyncio.create_task(follow_positions_loop())
    asyncio.create_task(position_close_loop(db_pool))

    # 🔹 Запуск слушателя задач (после полной инициализации)
    await listen_strategy_tasks(db_pool)
    
if __name__ == "__main__":
    asyncio.run(main())