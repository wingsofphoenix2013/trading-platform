import asyncio
import logging

logging.basicConfig(level=logging.INFO)

async def main_loop():
    logging.info("strategies_v2_main.py успешно запустился.")
    while True:
        logging.info("Основной цикл мониторинга активен.")
        await asyncio.sleep(5)  # задержка для цикла мониторинга

if __name__ == "__main__":
    asyncio.run(main_loop())