# indicators_live_main.py
# Координатор live-расчёта технических индикаторов по mark price для всех активных тикеров и таймфреймов

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
ohlcv_cache = {}  # структура: {symbol: {tf: DataFrame}}
smi_params = {}   # структура: {(symbol, tf): (k, d, s)}

# 3. Загрузка параметров SMI из indicator_settings
async def load_smi_params(pg_pool, symbol: str) -> tuple[int, int, int]:
    query = """
        SELECT param, value
        FROM indicator_settings
        WHERE indicator = 'smi' AND symbol = $1
    """
    try:
        rows = await pg_pool.fetch(query, symbol)
        params = {row['param']: int(row['value']) for row in rows}
        return params["k"], params["d"], params["s"]
    except Exception as e:
        print(f"[ERROR] Не удалось загрузить параметры SMI для {symbol}: {e}", flush=True)
        return 13, 5, 3

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
        df = df[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        print(f"[ERROR] Не удалось загрузить свечи {tf} для {symbol}: {e}", flush=True)
        return pd.DataFrame()

# 5. Синхронизация тикеров и инициализация кэша
async def sync_active_symbols(pg_pool):
    query = "SELECT symbol FROM tickers WHERE status = 'enabled'"
    try:
        rows = await pg_pool.fetch(query)
        symbols = [r["symbol"] for r in rows]
        tfs = ["M1", "M5", "M15"]
        for symbol in symbols:
            if symbol not in ohlcv_cache:
                ohlcv_cache[symbol] = {}
            for tf in tfs:
                if tf in ohlcv_cache[symbol]:
                    continue
                k, d, s = await load_smi_params(pg_pool, symbol)
                bars_needed = k + d + s + 10
                df = await load_last_n_bars(pg_pool, symbol, tf, bars_needed)
                if not df.empty:
                    ohlcv_cache[symbol][tf] = df
                    smi_params[(symbol, tf)] = (k, d, s)
                    print(f"[INIT] Загружено {len(df)} баров для {symbol}/{tf}", flush=True)
    except Exception as e:
        print(f"[ERROR] sync_active_symbols: {e}", flush=True)

# 6. Основная точка входа
async def main():
    print("[INIT] indicators_live_main стартует", flush=True)
    await asyncio.sleep(1)

    pg_pool = None
    redis = None

    try:
        pg_pool = await asyncpg.create_pool(DATABASE_URL)
        print("[PG] Подключение к PostgreSQL установлено", flush=True)

        redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )
        await redis.ping()
        print("[Redis] Подключение к Redis установлено", flush=True)

        await sync_active_symbols(pg_pool)
        asyncio.create_task(periodic_sync(pg_pool))
        await run_live_loop(redis)

    except Exception as e:
        print(f"[ERROR] Инициализация завершилась с ошибкой: {e}", flush=True)

    finally:
        if pg_pool:
            await pg_pool.close()
        if redis:
            await redis.close()


# 7. Периодическое обновление тикеров
async def periodic_sync(pg_pool):
    while True:
        await asyncio.sleep(300)
        await sync_active_symbols(pg_pool)

# 8. Главный цикл расчёта SMI по всем тикерам и TF
async def run_live_loop(redis):
    print("[LOOP] Запуск цикла расчёта SMI", flush=True)
    while True:
        try:
            await asyncio.sleep(2)
            for symbol in ohlcv_cache:
                for tf, df_base in ohlcv_cache[symbol].items():
                    price_key = f"price:{symbol}"
                    mark_price_raw = await redis.get(price_key)
                    if not mark_price_raw:
                        continue
                    try:
                        mark_price = float(mark_price_raw)
                    except:
                        continue
                    virtual_row = {"high": mark_price, "low": mark_price, "close": mark_price}
                    df_virtual = pd.concat([df_base, pd.DataFrame([virtual_row])], ignore_index=True)

                    k, d, s = smi_params.get((symbol, tf), (13, 5, 3))
                    result = await calculate_smi(df_virtual, k, d, s)
                    smi = result.get("smi")
                    smi_signal = result.get("smi_signal")

                    if smi is not None and smi_signal is not None:
                        redis_key = f"indicators_live:{symbol}:{tf}"
                        await redis.hset(redis_key, mapping={
                            "smi": smi,
                            "smi_signal": smi_signal
                        })
        except Exception as e:
            print(f"[ERROR] Ошибка в run_live_loop: {e}", flush=True)

# 9. Запуск
if __name__ == "__main__":
    asyncio.run(main())
