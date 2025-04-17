# indicators_main.py — расчёт технических индикаторов (исправленная версия с get_enabled_tickers)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis
import numpy as np
import json
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

# === Загрузка параметров индикаторов из базы ===
async def load_indicator_settings():
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT indicator, param, value FROM indicator_settings")
        await conn.close()

        result = {}
        for row in rows:
            ind = row["indicator"]
            param = row["param"]
            val = row["value"]
            if ind not in result:
                result[ind] = {}
            try:
                val = float(val)
                if val.is_integer():
                    val = int(val)
            except:
                pass
            result[ind][param] = val

        print(f"[SETTINGS] Loaded indicator settings: {result}", flush=True)
        return result

    except Exception as e:
        print(f"[ERROR] Failed to load indicator settings: {e}", flush=True)
        return {}

# === Получение списка активных тикеров ===
async def get_enabled_tickers():
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled' ORDER BY symbol ASC")
        await conn.close()
        return [row["symbol"] for row in rows]
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

# === Основной цикл воркера ===
async def main():
    print("[INIT] Starting indicators loop", flush=True)

    while True:
        now = datetime.utcnow()
        if now.minute % 5 == 0 and now.second < 5:
            print("[INFO] New M5 interval detected — starting indicator calculation", flush=True)

            settings = await load_indicator_settings()
            tickers = await get_enabled_tickers()

            for symbol in tickers:
                candles = await get_last_m5_candles(symbol, limit=100)

                angle = mid = upper = lower = rsi = smi_val = smi_signal_val = atr = None

                # === LR канал ===
                lr_len = settings.get("lr", {}).get("length", 50)
                angle_up = settings.get("lr", {}).get("angle_up", 2)
                angle_down = settings.get("lr", {}).get("angle_down", -2)
                trend = None

                if len(candles) >= lr_len:
                    closes = np.array([float(c["close"]) for c in candles[-lr_len:]])
                    x = np.arange(len(closes))
                    norm = (closes - closes.mean()) / closes.std()
                    slope, _ = np.polyfit(x, norm, 1)
                    angle = round(degrees(atan(slope)), 2)

                    if angle > angle_up:
                        trend = "up"
                    elif angle < angle_down:
                        trend = "down"
                    else:
                        trend = "flat"

                    slope_real, intercept_real = np.polyfit(x, closes, 1)
                    regression_line = slope_real * x + intercept_real
                    std_dev = np.std(closes - regression_line)
                    mid = round(regression_line[-1], 4)
                    upper = round(mid + 2 * std_dev, 4)
                    lower = round(mid - 2 * std_dev, 4)

                    print(f"[LR] {symbol}: angle={angle}°, trend={trend}, mid={mid}, upper={upper}, lower={lower}", flush=True)

                # === RSI ===
                rsi_period = settings.get("rsi", {}).get("period", 14)
                if len(candles) >= rsi_period + 1:
                    closes = np.array([float(c["close"]) for c in candles[-(rsi_period + 1):]])
                    deltas = np.diff(closes)
                    gain = np.where(deltas > 0, deltas, 0).sum() / rsi_period
                    loss = -np.where(deltas < 0, deltas, 0).sum() / rsi_period
                    rsi = 100.0 if loss == 0 else round(100 - (100 / (1 + gain / loss)), 2)
                    print(f"[RSI] {symbol}: RSI={rsi}", flush=True)

                # === SMI ===
                k = settings.get("smi", {}).get("k", 13)
                d = settings.get("smi", {}).get("d", 5)
                s = settings.get("smi", {}).get("s", 3)
                required = k + d + s
                if len(candles) >= required:
                    hlc3 = np.array([(float(c['high']) + float(c['low']) + float(c['close'])) / 3 for c in candles])
                    hh = np.array([max(hlc3[j-k:j]) for j in range(k, len(hlc3))])
                    ll = np.array([min(hlc3[j-k:j]) for j in range(k, len(hlc3))])
                    center = (hh + ll) / 2
                    diff = hlc3[k:] - center
                    smoothed_diff = np.convolve(diff, np.ones(d)/d, mode='valid')
                    smoothed_range = np.convolve(hh - ll, np.ones(d)/d, mode='valid')
                    smi_raw = 100 * smoothed_diff / (smoothed_range + 1e-9)
                    smi = np.convolve(smi_raw, np.ones(s)/s, mode='valid')
                    if len(smi) > 0:
                        smi_val = round(smi_raw[-1], 2)
                        smi_signal_val = round(smi[-1], 2)
                        print(f"[SMI] {symbol}: SMI={smi_val}, Signal={smi_signal_val}", flush=True)

                # === ATR ===
                atr_period = settings.get("atr", {}).get("period", 14)
                if len(candles) >= atr_period + 1:
                    highs = np.array([float(c['high']) for c in candles[-(atr_period + 1):]])
                    lows = np.array([float(c['low']) for c in candles[-(atr_period + 1):]])
                    closes = np.array([float(c['close']) for c in candles[-(atr_period + 1):]])
                    tr = [
                        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                        for i in range(1, len(highs))
                    ]
                    atr = round(sum(tr) / atr_period, 4)
                    print(f"[ATR] {symbol}: ATR={atr}", flush=True)

                # === Публикация в Redis ===
                await r.set(f"indicators:{symbol}", json.dumps({
                    "trend": trend,
                    "rsi": rsi,
                    "smi": smi_val,
                    "smi_signal": smi_signal_val,
                    "atr": atr,
                    "angle": angle,
                    "mid": mid,
                    "upper": upper,
                    "lower": lower
                }))

                # === Запись индикаторов в таблицу ohlcv_m5 ===
                if candles:
                    open_time = candles[-2]["open_time"]
                    db_url = os.getenv("DATABASE_URL")
                    try:
                        conn = await asyncpg.connect(dsn=db_url)
                        await conn.execute("""
                            UPDATE ohlcv_m5 SET
                                rsi = $1,
                                smi = $2,
                                smi_signal = $3,
                                atr = $4,
                                lr_angle = $5,
                                lr_mid = $6,
                                lr_upper = $7,
                                lr_lower = $8,
                                lr_trend = $9
                            WHERE symbol = $10 AND open_time = $11
                        """,
                        rsi, smi_val, smi_signal_val, atr,
                        angle, mid, upper, lower, trend,
                        symbol, open_time)
                        await conn.close()
                        print(f"[DB] Обновлена свеча {symbol} @ {open_time} с индикаторами", flush=True)
                    except Exception as e:
                        print(f"[ERROR] Не удалось обновить ohlcv_m5 для {symbol}: {e}", flush=True)

            await asyncio.sleep(5)
        else:
            await asyncio.sleep(1)

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(main())
