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

# 3. Загрузка параметров SMI из indicator_settings
async def load_smi_params(pg_pool) -> tuple[int, int, int]:
    query = """
        SELECT param, value
        FROM indicator_settings
        WHERE indicator = 'smi'
    """
    try:
        rows = await pg_pool.fetch(query)
        params = {row["param"]: int(row["value"]) for row in rows}
        k = params["k"]
        d = params["d"]
        s = params["s"]
        print(f"[PARAMS] Параметры SMI загружены: k={k}, d={d}, s={s}", flush=True)
        return k, d, s
    except Exception as e:
        print(f"[ERROR] Не удалось загрузить параметры SMI: {e}", flush=True)
        return 13, 5, 3  # значения по умолчанию (если что-то пошло не так)

# 4. Загрузка последних N баров из базы
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

# 5. Основная точка входа
async def main():
    print("[INIT] indicators_live_main стартует", flush=True)
    await asyncio.sleep(1)

    pg_pool = None
    redis = None

    try:
        # 5.1 Подключение к PostgreSQL
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        print("[PG] Подключение к PostgreSQL установлено", flush=True)

        # 5.2 Подключение к Redis
        redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )
        await redis.ping()
        print("[Redis] Подключение к Redis установлено", flush=True)

        # 5.3 Загрузка параметров SMI
        k, d, s = await load_smi_params(pg_pool)
        bars_needed = k + d + s + 10

        # 5.4 Загрузка свечей ADAUSDT / M5
        symbol = "ADAUSDT"
        tf = "M5"
        df = await load_last_n_bars(pg_pool, symbol, tf, limit=bars_needed)
        ohlcv_cache[symbol] = {tf: df}
        print(f"[CACHE] Загружено {len(df)} баров для {symbol} / {tf}", flush=True)

        # 5.5 Запуск главного цикла
        await run_live_loop(pg_pool, redis, symbol, tf, k, d, s)

    except Exception as e:
        print(f"[ERROR] Инициализация завершилась с ошибкой: {e}", flush=True)

    finally:
        if pg_pool:
            await pg_pool.close()
        if redis:
            await redis.close()


# 6. Главный рабочий цикл с расчётом SMI
async def run_live_loop(pg_pool, redis, symbol, tf, k, d, s):
    print("[LOOP] Запуск основного цикла обработки индикаторов", flush=True)
    while True:
        try:
            await asyncio.sleep(2)

            # 6.1 Получение mark price
            price_key = f"price:{symbol}"
            mark_price_raw = await redis.get(price_key)
            if not mark_price_raw:
                print(f"[WARN] Нет данных mark_price для {symbol}", flush=True)
                continue

            mark_price = float(mark_price_raw)

            # 6.2 Виртуальный бар
            df_base = ohlcv_cache[symbol][tf].copy()
            virtual_row = {"high": mark_price, "low": mark_price, "close": mark_price}
            df_virtual = pd.concat([df_base, pd.DataFrame([virtual_row])], ignore_index=True)

            # 6.3 Расчёт SMI
            result = await calculate_smi(df_virtual, k, d, s)
            smi = result.get("smi")
            smi_signal = result.get("smi_signal")

            if smi is not None and smi_signal is not None:
                redis_key = f"indicators_live:{symbol}:{tf}"
                await redis.hset(redis_key, mapping={
                    "smi": smi,
                    "smi_signal": smi_signal
                })
                print(f"[REDIS] Обновлён {redis_key} → smi={smi}, signal={smi_signal}", flush=True)
            else:
                print(f"[ERROR] Не удалось рассчитать SMI для {symbol}/{tf}", flush=True)

        except Exception as e:
            print(f"[ERROR] Ошибка в run_live_loop: {e}", flush=True)


# 7. Запуск
if __name__ == "__main__":
    asyncio.run(main())
