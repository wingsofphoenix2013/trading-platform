import asyncio
import logging
import redis.asyncio as redis
import os
import sys

# Настройка логирования с немедленным flush в stdout
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Конфигурация окружения ---
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Временный список тикеров для мониторинга
SYMBOLS = ["BTCUSDT", "AVAXUSDT"]

async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")

    # Асинхронное подключение к Redis с SSL (требование Upstash)
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        ssl=True
    )

    while True:
        for symbol in SYMBOLS:
            try:
                # Получение текущей цены тикера из Redis
                price = await r.get(f'price:{symbol}')
                logging.info(f"Текущая цена {symbol}: {price}")
            except Exception as e:
                logging.error(f"Ошибка при получении цены {symbol} из Redis: {e}")

        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main_loop())