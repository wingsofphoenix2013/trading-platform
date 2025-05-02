# 🔸 Импорты и базовая настройка
import asyncio
import logging

from interface import StrategyInterface

logging.basicConfig(level=logging.INFO)

# 🔸 Хранилища в памяти
tickers_storage = {}
open_positions = {}
latest_prices = {}

# 🔸 Загрузка тикеров из БД
async def load_tickers():
    interface = StrategyInterface()
    global tickers_storage
    tickers_storage = await interface.load_tickers()
    logging.info(f"✅ Загружено тикеров: {len(tickers_storage)}")

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
    asyncio.create_task(refresh_tickers_periodically())
    asyncio.create_task(monitor_prices())
    await listen_strategy_tasks()

if __name__ == "__main__":
    asyncio.run(main())