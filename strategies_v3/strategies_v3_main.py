# 🔸 Импорты и базовая настройка
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN

# 🔸 Импорт интерфейса стратегии
from strategies_v3_interface import StrategyInterface

# 🔸 Импорт логики стратегии (класс)
from strategy_1 import Strategy1

logging.basicConfig(level=logging.INFO)

# 🔸 Хранилища в памяти
tickers_storage = {}
open_positions = {}
latest_prices = {}

# 🔸 Хранилища стратегий
strategies_cache = {}
allowed_symbols = {}

# 🔸 Зарегистрированные стратегии v3
strategies = {}

# 🔸 Загрузка тикеров из БД
async def load_tickers(interface: StrategyInterface):
    global tickers_storage
    tickers_storage = await interface.load_tickers()
    logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")

# 🔸 Загрузка стратегий
async def load_strategies(interface: StrategyInterface):
    pg = await interface.get_pg()
    rows = await pg.fetch("""
        SELECT * FROM strategies_v2
        WHERE enabled = true AND archived = false
    """)
    global strategies_cache
    strategies_cache = {row["name"]: dict(row) for row in rows}
    logging.info(f"✅ Загружено стратегий: {len(strategies_cache)}")

    # 🔹 Диагностика
    for name in strategies_cache.keys():
        logging.info(f"📦 Стратегия загружена: {name}")

# 🔸 Загрузка тикеров, разрешённых для стратегий
async def load_strategy_tickers(interface: StrategyInterface):
    pg = await interface.get_pg()

    # 🔹 Загружаем тикеры с разрешённой торговлей
    ticker_rows = await pg.fetch("""
        SELECT symbol FROM tickers
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """)
    all_symbols = {row["symbol"] for row in ticker_rows}

    # 🔹 Загружаем стратегии с use_all_tickers = true
    global allowed_symbols
    allowed_symbols.clear()

    strategy_all_rows = await pg.fetch("""
        SELECT name FROM strategies_v2
        WHERE enabled = true AND archived = false AND use_all_tickers = true
    """)
    for row in strategy_all_rows:
        allowed_symbols[row["name"]] = set(all_symbols)

    # 🔹 Загружаем связи из strategy_tickers_v2 для остальных стратегий
    specific_rows = await pg.fetch("""
        SELECT s.name AS strategy_name, t.symbol
        FROM strategy_tickers_v2 st
        JOIN strategies_v2 s ON s.id = st.strategy_id
        JOIN tickers t ON t.id = st.ticker_id
        WHERE st.enabled = true AND t.status = 'enabled' AND t.tradepermission = 'enabled'
    """)
    for row in specific_rows:
        strategy = row["strategy_name"]
        symbol = row["symbol"]
        allowed_symbols.setdefault(strategy, set()).add(symbol)

    total = sum(len(v) for v in allowed_symbols.values())
    logging.info(f"✅ Разрешённые тикеры загружены для {len(allowed_symbols)} стратегий, всего связей: {total}")
# 🔸 Периодическое обновление тикеров и разрешений
async def refresh_tickers_periodically(interface: StrategyInterface):
    while True:
        try:
            await load_tickers(interface)
            await load_strategy_tickers(interface)
        except Exception as e:
            logging.error(f"Ошибка обновления тикеров/разрешений: {e}")
        await asyncio.sleep(120)

# 🔸 Мониторинг цен из Redis
async def monitor_prices(interface: StrategyInterface):
    logging.info("🔄 monitor_prices ЗАПУЩЕН")

    interface = StrategyInterface()
    redis = await interface.get_redis()

    while True:
        try:
            for symbol in tickers_storage.keys():
                raw_price = await redis.get(f"price:{symbol}")
                if raw_price:
                    try:
                        latest_prices[symbol] = Decimal(raw_price)
                    except Exception as conv_err:
                        logging.warning(f"❗ Ошибка конвертации цены {symbol}: {raw_price} → {conv_err}")
        except Exception as e:
            logging.error(f"Ошибка обновления цен из Redis: {e}")
        await asyncio.sleep(1)
# 🔸 Слушатель Redis Stream strategy_tasks
async def listen_strategy_tasks(interface: StrategyInterface):
    redis = await interface.get_redis()
    group = "strategy_worker_group"
    consumer = "worker-1"

    try:
        await redis.xgroup_create("strategy_tasks", group, id="0", mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        try:
            messages = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={"strategy_tasks": ">"},
                count=10,
                block=500
            )
            for stream_name, msgs in messages:
                for msg_id, msg_data in msgs:
                    task = {k: v for k, v in msg_data.items()}
                    logging.info(f"📥 Получена задача: {task}")
                    await handle_task(task, interface)
                    await redis.xack("strategy_tasks", group, msg_id)
        except Exception as e:
            logging.error(f"Ошибка чтения из Redis Stream: {e}")
            await asyncio.sleep(1)
# 🔸 Обработка одной задачи из Redis Stream
async def handle_task(task_data: dict, interface: StrategyInterface):
    try:
        strategy_name = task_data["strategy"]
        symbol = task_data["symbol"]
        direction = task_data["direction"]
        log_id_raw = task_data.get("log_id")
        log_id = int(log_id_raw) if log_id_raw is not None else -1

        strategy = strategies_cache.get(strategy_name)
        strategy_id = strategy["id"] if strategy else None

        logging.info(f"🧩 Задача: {task_data}")
        logging.info(f"🧩 strategies_cache.keys(): {list(strategies_cache.keys())}")
        logging.info(f"🧩 strategies.keys(): {list(strategies.keys())}")
        
        # 🔹 Защита по стратегии
        if strategy is None:
            await interface.log_strategy_action(
                log_id=log_id,
                strategy_id=None,
                status="error",
                note=f"Стратегия {strategy_name} не найдена в кеше"
            )
            return
        # 🔹 Проверка тикера
        if symbol not in tickers_storage:
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="ignored_by_check", note="Тикер не найден в tickers_storage"
            )
            return

        if tickers_storage[symbol]["tradepermission"] != "enabled":
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="ignored_by_check", note="Торговля тикером запрещена"
            )
            return

        if symbol not in allowed_symbols.get(strategy_name, set()):
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="ignored_by_check", note="Тикер не разрешён для стратегии"
            )
            return

        if (strategy_name, symbol) in open_positions:
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="ignored_by_check", note="Позиция уже открыта"
            )
            return

        # 🔹 Получение цены
        entry_price = latest_prices.get(symbol)
        if not entry_price:
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="error", note="Нет актуальной цены в Redis"
            )
            return

        # 🔹 Вызов логики стратегии из зарегистрированного экземпляра
        if strategy_name not in strategies:
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="error", note=f"Неизвестная или не зарегистрированная стратегия: {strategy_name}"
            )
            return

        strategy_instance = strategies[strategy_name]
        signal_result = await strategy_instance.on_signal(task_data)

        if signal_result.get("action") != "open":
            await interface.log_strategy_action(
                log_id=log_id, strategy_id=strategy_id,
                status="ignored_by_check", note="Стратегия отклонила сигнал"
            )
            return

        # 🔹 Открытие позиции
        position_id = await interface.open_position(strategy_name, symbol, direction, entry_price, log_id)

        await interface.log_strategy_action(
            log_id=log_id, strategy_id=strategy_id,
            status="position_opened", position_id=position_id
        )

    except Exception as e:
        import traceback

        strategy_id = None
        strategy = strategies_cache.get(task_data.get("strategy"))
        if strategy:
            strategy_id = strategy.get("id")

        log_id_raw = task_data.get("log_id")
        log_id = int(log_id_raw) if log_id_raw is not None else -1

        logging.error(f"🐞 Ошибка при обработке сигнала {task_data.get('strategy')}: {e}")
        logging.error(traceback.format_exc())

        await interface.log_strategy_action(
            log_id=log_id,
            strategy_id=strategy_id,
            status="error",
            note=f"Ошибка при обработке: {e}"
    )
    
# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")

    interface = StrategyInterface(strategies_cache)

    await load_tickers(interface)
    await load_strategies(interface)
    await load_strategy_tickers(interface)

    strategies["strategy_1"] = Strategy1(interface)

    asyncio.create_task(refresh_tickers_periodically(interface))
    asyncio.create_task(monitor_prices(interface))

    await listen_strategy_tasks(interface)

if __name__ == "__main__":
    asyncio.run(main())