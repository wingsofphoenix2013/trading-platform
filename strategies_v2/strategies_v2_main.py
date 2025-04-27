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

REDIS_URL = os.getenv("REDIS_URL")
SYMBOLS = ["BTCUSDT", "AVAXUSDT"]

async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")
    r = redis.from_url(REDIS_URL)

    while True:
        for symbol in SYMBOLS:
            try:
                price = await r.get(f'price:{symbol}')
                logging.info(f"Текущая цена {symbol}: {price}")
            except Exception as e:
                logging.error(f"Ошибка при получении цены {symbol} из Redis: {e}")

        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main_loop())