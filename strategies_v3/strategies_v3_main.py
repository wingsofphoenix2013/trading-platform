# 🔸 Импорты и базовая настройка
import asyncio
import logging

# 🔸 Импорт интерфейса стратегии
from strategies_v3_interface import StrategyInterface

logging.basicConfig(level=logging.INFO)

# 🔸 Хранилища в памяти
tickers_storage = {}
open_positions = {}
latest_prices = {}
# 🔸 Хранилища стратегий
strategies_cache = {}
allowed_symbols = {}

# 🔸 Загрузка тикеров из БД
async def load_tickers():
    interface = StrategyInterface()
    global tickers_storage
    tickers_storage = await interface.load_tickers()
    logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")

# 🔸 Загрузка стратегий
async def load_strategies():
    interface = StrategyInterface()
    pg = await interface.get_pg()
    rows = await pg.fetch("""
        SELECT * FROM strategies_v2
        WHERE enabled = true AND archived = false
    """)
    global strategies_cache
    strategies_cache = {row["name"]: dict(row) for row in rows}
    logging.info(f"✅ Загружено стратегий: {len(strategies_cache)}")

# 🔸 Загрузка тикеров, разрешённых для стратегий
async def load_strategy_tickers():
    interface = StrategyInterface()
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
async def refresh_tickers_periodically():
    while True:
        try:
            await load_tickers()
            await load_strategy_tickers()
        except Exception as e:
            logging.error(f"Ошибка обновления тикеров/разрешений: {e}")
        await asyncio.sleep(120)

# 🔸 Мониторинг цен (заглушка)
async def monitor_prices():
    while True:
        await asyncio.sleep(1)
# 🔸 Слушатель Redis Stream strategy_tasks
async def listen_strategy_tasks():
    interface = StrategyInterface()
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
                    await handle_task(task)
                    await redis.xack("strategy_tasks", group, msg_id)
        except Exception as e:
            logging.error(f"Ошибка чтения из Redis Stream: {e}")
            await asyncio.sleep(1)
# 🔸 Обработка одной задачи из Redis Stream
async def handle_task(task_data: dict):
    try:
        interface = StrategyInterface()
        strategy_name = task_data["strategy"]
        symbol = task_data["symbol"]
        direction = task_data["direction"]
        log_id_raw = task_data.get("log_id")
        log_id = int(log_id_raw) if log_id_raw is not None else -1

        strategy = strategies_cache.get(strategy_name)
        strategy_id = strategy["id"] if strategy else None

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

        # 🔹 Вызов логики стратегии
        mod = __import__(f"strategies_v3.{strategy_name}", fromlist=["on_signal"])
        signal_result = await mod.on_signal(task_data, interface)

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
        await interface.log_strategy_action(
            log_id=log_id,
            strategy_id=strategies_cache.get(task_data.get("strategy"), {}).get("id"),
            status="error",
            note=f"Ошибка при обработке: {e}"
        )
# 🔸 Главная точка запуска
async def main():
    logging.info("🚀 Strategy Worker (v3) запущен.")
    await load_tickers()
    await load_strategies()
    await load_strategy_tickers()
    asyncio.create_task(refresh_tickers_periodically())
    asyncio.create_task(monitor_prices())
    await listen_strategy_tasks()

if __name__ == "__main__":
    asyncio.run(main())