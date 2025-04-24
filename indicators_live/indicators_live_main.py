# indicators_live_main.py
# Координатор live-расчёта технических индикаторов по mark price

# 0. Импорты
import asyncio
import asyncpg
import redis.asyncio as aioredis
import os
import json
import pandas as pd
from datetime import datetime
from smi_live import calculate_smi

# 1. Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# 2. Глобальный кэш свечей (в памяти)
ohlcv_cache = {}  # структура: {symbol: {"M1": df, "M5": df, "M15": df}}

# 3. Загрузка последних N баров из базы
async def load_last_n_bars(pg_pool, symbol: str, tf: str, limit: int = 100) -> pd.DataFrame:
    table = f"ohlcv_{tf.lower()}"
    query = f"""
        SELECT open_time, high, low, close
        FROM {table}
        WHERE symbol = $1
        ORDER BY open_time DESC
        LIMIT {limit}
    """
    try:
        rows = await pg_pool.fetch(query, symbol)
        df = pd.DataFrame(rows, columns=["open_time", "high", "low", "close"])
        df = df[::-1].reset_index(drop=True)  # разворот по времени (свежие внизу)
        return df
    except Exception as e:
        print(f"[ERROR] Не удалось загрузить свечи {tf} для {symbol}: {e}", flush=True)
        return pd.DataFrame()

# 4. Основная точка входа
async def main():
    print("[INIT] indicators_live_main стартует", flush=True)
    await asyncio.sleep(1)

    pg_pool = None
    redis = None

    try:
        # 4.1 Подключение к PostgreSQL
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        print("[PG] Подключение к PostgreSQL установлено", flush=True)

        # 4.2 Подключение к Redis
        redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )
        await redis.ping()
        print("[Redis] Подключение к Redis установлено", flush=True)

        # 4.3 Запуск главного цикла (заглушка)
        await run_live_loop(pg_pool, redis)

    except Exception as e:
        print(f"[ERROR] Инициализация завершилась с ошибкой: {e}", flush=True)

    finally:
        if pg_pool:
            await pg_pool.close()
        if redis:
            await redis.close()


# 5. Главный рабочий цикл (заглушка)
async def run_live_loop(pg_pool, redis):
    print("[LOOP] Запуск основного цикла обработки индикаторов", flush=True)
    while True:
        await asyncio.sleep(2)
        print("[LOOP] Псевдо-расчёт завершён", flush=True)


# 6. Запуск
if __name__ == "__main__":
    asyncio.run(main())
