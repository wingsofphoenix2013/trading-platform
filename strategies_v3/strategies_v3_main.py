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
    rows = await pg.fetch("""
        SELECT s.name AS strategy_name, t.symbol
        FROM strategy_tickers_v2 st
        JOIN strategies_v2 s ON s.id = st.strategy_id
        JOIN tickers t ON t.id = st.ticker_id
        WHERE st.enabled = true
    """)
    global allowed_symbols
    allowed_symbols.clear()
    for row in rows:
        strategy = row["strategy_name"]
        symbol = row["symbol"]
        allowed_symbols.setdefault(strategy, set()).add(symbol)
    logging.info(f"✅ Загружено связей стратегия-тикер: {len(rows)}")
# 🔸 Периодическое обновление тикеров
async def refresh_tickers_periodically():
    while True:
        try:
            await load_tickers()
        except Exception as e:
            logging.error(f"Ошибка обновления тикеров: {e}")
        await asyncio.sleep(300)

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