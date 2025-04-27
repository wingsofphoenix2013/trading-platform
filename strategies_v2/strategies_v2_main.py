import asyncio
import logging
import redis.asyncio as redis
import os
import sys
import asyncpg

# Настройка логирования с немедленным flush в stdout
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Конфигурация Базы Данных ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Проверка подключения к PostgreSQL
async def test_db_connection():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        logging.info("Успешное подключение к базе данных.")

        # Простой тестовый запрос
        row = await conn.fetchrow('SELECT 1 AS test_field;')
        logging.info(f"Тестовый запрос успешен, получено: {row['test_field']}")

        await conn.close()
    except Exception as e:
        logging.error(f"Ошибка подключения к БД: {e}")

# Список тикеров для мониторинга (позже загружается из базы)
SYMBOLS = ["BTCUSDT", "AVAXUSDT"]

# Импорт и регистрация стратегий
from strategy_1 import Strategy1

strategies = {
    "strategy_1": Strategy1()
}

# Асинхронный цикл мониторинга текущих цен
def log_price(symbol, price):
    logging.info(f"Текущая цена {symbol}: {price}")

async def monitor_prices(redis_client):
    while True:
        for symbol in SYMBOLS:
            try:
                price = await redis_client.get(f'price:{symbol}')
                log_price(symbol, price)
            except Exception as e:
                logging.error(f"Ошибка при получении цены {symbol} из Redis: {e}")

        await asyncio.sleep(5)

# Асинхронная подписка на сигналы и их передача стратегиям
async def listen_signals(redis_client):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe('incoming_signals')
    logging.info("Подписка на канал incoming_signals выполнена.")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            signal_data = message['data']
            logging.info(f"Получен сигнал: {signal_data}")

            # Передача сигнала всем стратегиям
            for strategy in strategies.values():
                await strategy.on_signal(signal_data)

# Основной цикл приложения
async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")

    # Проверка базы данных при запуске
    await test_db_connection()

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        ssl=True
    )

    # Пока оставляем только мониторинг цен (сигналы обработаем отдельно позже)
    await monitor_prices(redis_client)

# Запуск основного цикла
if __name__ == "__main__":
    asyncio.run(main_loop())