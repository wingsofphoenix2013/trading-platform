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

import json

# Асинхронная функция подписки, парсинга и проверки сигналов по БД
async def listen_signals(redis_client):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe('incoming_signals')
    logging.info("Подписка на канал incoming_signals выполнена.")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            signal_data = message['data']
            logging.info(f"Получен сигнал: {signal_data}")

            # Парсинг сигнала
            try:
                signal_json = json.loads(signal_data)
                signal_text = signal_json.get("message", "")
                source = signal_json.get("source", "")

                phrase, symbol = signal_text.strip().split(" ")
                phrase = phrase.strip().upper()
                symbol = symbol.strip().upper()

                logging.info(f"Парсинг успешен — Фраза: '{phrase}', Тикер: '{symbol}', Источник: '{source}'")

                # Проверка сигнала по базе
                signal_row = await check_signal_in_db(phrase)
                if signal_row:
                    logging.info(f"Сигнал '{phrase}' успешно найден и активен (id={signal_row['id']}).")
                else:
                    logging.warning(f"Сигнал '{phrase}' не найден или неактивен. Игнорируется.")

            except Exception as e:
                logging.error(f"Ошибка обработки сигнала: {e}")

# Функция проверки сигнала в таблице signals (без учёта source)
async def check_signal_in_db(phrase):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        query = """
        SELECT id, enabled FROM signals 
        WHERE (long_phrase=$1 OR short_phrase=$1 OR long_exit_phrase=$1 OR short_exit_phrase=$1)
        AND enabled=true
        LIMIT 1
        """
        signal_row = await conn.fetchrow(query, phrase)
        return signal_row
    except Exception as e:
        logging.error(f"Ошибка при запросе к signals: {e}")
        return None
    finally:
        await conn.close()
        
# Основной цикл приложения
async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")

    await test_db_connection()

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        ssl=True
    )

    # Параллельный запуск мониторинга цен и подписки на сигналы
    await asyncio.gather(
        monitor_prices(redis_client),
        listen_signals(redis_client)
    )

# Запуск основного цикла
if __name__ == "__main__":
    asyncio.run(main_loop())