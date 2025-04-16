# indicators_main.py — расчёт технических индикаторов (с LR, RSI, SMI, ATR)

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
            SELECT open_time, high, low, close FROM ohlcv_m5
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

    closes = np.array([float(c["close"]) for c in candles[-length:]])
    x = np.arange(len(closes))

    norm = (closes - closes.mean()) / closes.std()
    slope, intercept = np.polyfit(x, norm, 1)
    angle = degrees(atan(slope))

    slope_real, intercept_real = np.polyfit(x, closes, 1)
    regression_line = slope_real * x + intercept_real
    std_dev = np.std(closes - regression_line)
    mid = regression_line[-1]
    upper = mid + std_multiplier * std_dev
    lower = mid - std_multiplier * std_dev

    print(f"[LR] {symbol}: angle={angle:.2f}°, slope(norm)={slope:.4f}, mid={mid:.4f}, upper={upper:.4f}, lower={lower:.4f}", flush=True)

# === Расчёт RSI по close ===
def calculate_rsi(symbol, candles, period=14):
    if len(candles) < period:
        print(f"[SKIP] {symbol}: not enough candles for RSI (have {len(candles)}, need {period})", flush=True)
        return

    closes = np.array([float(c["close"]) for c in candles[-(period + 1):]])
    deltas = np.diff(closes)
    gain = np.where(deltas > 0, deltas, 0).sum() / period
    loss = -np.where(deltas < 0, deltas, 0).sum() / period

    if loss == 0:
        rsi = 100.0
    else:
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

    print(f"[RSI] {symbol}: RSI={rsi:.2f}", flush=True)

# === Расчёт SMI по hlc3 ===
def calculate_smi(symbol, candles, k=13, d=5, s=3):
    required = k + d + s
    if len(candles) < required:
        print(f"[SKIP] {symbol}: not enough candles for SMI (have {len(candles)}, need {required})", flush=True)
        return

    hlc3 = np.array([(float(c['high']) + float(c['low']) + float(c['close'])) / 3 for c in candles])

    highest_high = np.array([max([hlc3[j] for j in range(i - k, i)]) for i in range(k, len(hlc3))])
    lowest_low = np.array([min([hlc3[j] for j in range(i - k, i)]) for i in range(k, len(hlc3))])
    center = (highest_high + lowest_low) / 2
    diff = hlc3[k:len(hlc3)] - center

    smoothed_diff = np.convolve(diff, np.ones(d)/d, mode='valid')
    smoothed_range = np.convolve(highest_high - lowest_low, np.ones(d)/d, mode='valid')

    smi_raw = 100 * smoothed_diff / (smoothed_range + 1e-9)
    smi = np.convolve(smi_raw, np.ones(s)/s, mode='valid')

    if len(smi) > 0:
        print(f"[SMI] {symbol}: SMI={smi_raw[-1]:.2f}, Signal={smi[-1]:.2f}", flush=True)
    else:
        print(f"[SKIP] {symbol}: SMI smoothing produced no output", flush=True)

# === Расчёт ATR ===
def calculate_atr(symbol, candles, period=14):
    if len(candles) < period + 1:
        print(f"[SKIP] {symbol}: not enough candles for ATR (have {len(candles)}, need {period + 1})", flush=True)
        return

    highs = np.array([float(c['high']) for c in candles[-(period + 1):]])
    lows = np.array([float(c['low']) for c in candles[-(period + 1):]])
    closes = np.array([float(c['close']) for c in candles[-(period + 1):]])

    tr_list = []
    for i in range(1, len(highs)):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        tr_list.append(tr)

    atr = sum(tr_list) / period
    print(f"[ATR] {symbol}: ATR={atr:.4f}", flush=True)

# === Основной цикл воркера ===
async def main():
    print("[INIT] Starting indicators loop", flush=True)

    while True:
        now = datetime.utcnow()
        if now.minute % 5 == 0 and now.second < 5:
            print("[INFO] New M5 interval detected — starting indicator calculation", flush=True)

            tickers = await get_enabled_tickers()

            for symbol in tickers:
                candles = await get_last_m5_candles(symbol, limit=100)
                calculate_lr_channel(symbol, candles)
                calculate_rsi(symbol, candles)
                calculate_smi(symbol, candles)
                calculate_atr(symbol, candles)

            await asyncio.sleep(5)
        else:
            await asyncio.sleep(1)

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(main())
