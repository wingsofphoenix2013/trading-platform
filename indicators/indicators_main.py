# indicators_main.py — расчёт технических индикаторов (каркас с логами)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis
from datetime import datetime

# === Подключение к Redis ===
print("[INIT] Connecting to Redis...", flush=True)
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)
print("[OK] Connected to Redis", flush=True)

# === Подключение к PostgreSQL ===
async def get_enabled_tickers():
    print("[DB] Fetching enabled tickers...", flush=True)
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled'")
        await conn.close()
        symbols = [row["symbol"] for row in rows]
        print(f"[DB] {len(symbols)} tickers fetched: {symbols}", flush=True)
        return symbols
    except Exception as e:
        print(f"[ERROR] DB connection failed: {e}", flush=True)
        return []

# === Получение последних N свечей M5 по тикеру ===
async def get_last_m5_candles(symbol, limit=100):
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch(
            """
            SELECT * FROM ohlcv_m5
            WHERE symbol = $1
            ORDER BY open_time DESC
            LIMIT $2
            """,
            symbol, limit
        )
        await conn.close()

        if rows:
            sorted_rows = sorted(rows, key=lambda r: r["open_time"])
            print(f"[DATA] {symbol}: {len(sorted_rows)} candles fetched", flush=True)
            print(f"        from {sorted_rows[0]['open_time']} to {sorted_rows[-1]['open_time']}", flush=True)
        else:
            print(f"[WARN] {symbol}: no M5 candles found", flush=True)

        return rows

    except Exception as e:
        print(f"[ERROR] Failed to fetch M5 candles for {symbol}: {e}", flush=True)
        return []

# === Основной цикл воркера ===
async def main():
    print("[INIT] Starting indicators loop", flush=True)
    while True:
        tickers = await get_enabled_tickers()
        print("[INFO] Starting calculation cycle (fetching M5 candles)", flush=True)

        for symbol in tickers:
            await get_last_m5_candles(symbol)

        await asyncio.sleep(300)  # Пауза 5 минут

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(main())
