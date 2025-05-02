# 🔸 Импорты и базовая настройка
import asyncio
import logging

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

# 🔸 Основной цикл получения задач
async def listen_strategy_tasks():
    while True:
        await asyncio.sleep(1)

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