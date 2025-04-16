# indicators_main.py — расчёт технических индикаторов (с логикой линейной регрессии)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis
import numpy as np
from datetime import datetime
from math import atan, degrees

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
            SELECT open_time, close FROM ohlcv_m5
            WHERE symbol = $1
            ORDER BY open_time DESC
            LIMIT $2
            """,
            symbol, limit
        )
        await conn.close()

        if rows:
            sorted_rows = sorted(rows, key=lambda r: r["open_time"])
            return sorted_rows
        else:
            return []

    except Exception as e:
        print(f"[ERROR] Failed to fetch M5 candles for {symbol}: {e}", flush=True)
        return []

# === Расчёт линейного регрессионного канала ===
def calculate_lr_channel(symbol, candles, length=50, std_multiplier=2):
    if len(candles) < length:
        print(f"[SKIP] {symbol}: not enough candles for regression (have {len(candles)}, need {length})", flush=True)
        return

    closes = np.array([c["close"] for c in candles[-length:]])
    x = np.arange(len(closes))

    # === 1. Угол по нормализованным close ===
    norm = (closes - closes.mean()) / closes.std()
    slope, intercept = np.polyfit(x, norm, 1)
    angle = degrees(atan(slope))

    # === 2. Границы канала по реальным close ===
    slope_real, intercept_real = np.polyfit(x, closes, 1)
    regression_line = slope_real * x + intercept_real
    std_dev = np.std(closes - regression_line)
    mid = regression_line[-1]
    upper = mid + std_multiplier * std_dev
    lower = mid - std_multiplier * std_dev

    print(f"[LR] {symbol}: angle={angle:.2f}°, slope(norm)={slope:.4f}, mid={mid:.2f}, upper={upper:.2f}, lower={lower:.2f}", flush=True)

# === Основной цикл воркера ===
async def main():
    print("[INIT] Starting indicators loop", flush=True)
    while True:
        tickers = await get_enabled_tickers()
        print("[INFO] Starting calculation cycle (LR channel)", flush=True)

        for symbol in tickers:
            candles = await get_last_m5_candles(symbol, limit=100)
            calculate_lr_channel(symbol, candles)

        await asyncio.sleep(300)  # Пауза 5 минут

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(main())
