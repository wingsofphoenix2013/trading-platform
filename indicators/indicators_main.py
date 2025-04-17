# indicators_main.py — шаг 6.5: расчёт RSI, SMI и LR без изменений

import asyncio
import json
import math
import os
import numpy as np
from math import atan, degrees
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
import redis.asyncio as redis

# Подключение к БД
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

ohlcv_table = metadata.tables['ohlcv_m5']
settings_table = metadata.tables['indicator_settings']

# Подключение к Redis
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True,
    decode_responses=True
)

# Расчёт индикаторов
async def process_candle(symbol, timestamp):
    print(f"[DEBUG] ВХОД: process_candle(symbol={symbol}, timestamp={timestamp})", flush=True)

    try:
        result = session.execute(select(settings_table)).fetchall()
        if not result:
            print(f"[ERROR] Таблица indicator_settings пуста", flush=True)
            return

        settings = {}
        for row in result:
            row_dict = dict(row._mapping)
            indicator = row_dict.get("indicator")
            param = row_dict.get("param")
            value = row_dict.get("value")

            if not indicator or not param:
                continue

            if indicator not in settings:
                settings[indicator] = {}

            try:
                value = float(value)
                if value.is_integer():
                    value = int(value)
            except:
                pass

            settings[indicator][param] = value

        print(f"[DEBUG] Построенные настройки: {settings}", flush=True)

        lr_len = settings.get("lr", {}).get("length", 50)
        angle_up = settings.get("lr", {}).get("angle_up", 2)
        angle_down = settings.get("lr", {}).get("angle_down", -2)
        rsi_period = settings.get("rsi", {}).get("period", 14)
        smi_k = settings.get("smi", {}).get("k", 13)
        smi_d = settings.get("smi", {}).get("d", 5)
        smi_s = settings.get("smi", {}).get("s", 3)

        lookback = max(rsi_period, smi_k + smi_d + smi_s, lr_len)

        candles = session.query(ohlcv_table) \
            .filter(ohlcv_table.c.symbol == symbol) \
            .filter(ohlcv_table.c.open_time <= timestamp) \
            .filter(ohlcv_table.c.complete == True) \
            .order_by(ohlcv_table.c.open_time.desc()) \
            .limit(lookback) \
            .all()

        candles = list(reversed(candles))

        if len(candles) < lookback:
            print(f"[WARNING] Недостаточно данных для индикаторов ({len(candles)} < {lookback})", flush=True)
            return

        closes = [float(c.close) for c in candles]
        highs = [float(c.high) for c in candles]
        lows = [float(c.low) for c in candles]

        # === RSI ===
        gains, losses = [], []
        for i in range(-rsi_period - 1, -1):
            delta = closes[i + 1] - closes[i]
            gains.append(max(delta, 0))
            losses.append(abs(min(delta, 0)))

        avg_gain = sum(gains) / rsi_period
        avg_loss = sum(losses) / rsi_period
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100
        print(f"[DEBUG] RSI для {symbol} @ {timestamp}: {rsi:.2f}", flush=True)

        # === SMI ===
        k, d, s = smi_k, smi_d, smi_s
        required = k + d + s

        if len(candles) >= required:
            hlc3 = np.array([(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)])
            hh = np.array([max(hlc3[j - k:j]) for j in range(k, len(hlc3))])
            ll = np.array([min(hlc3[j - k:j]) for j in range(k, len(hlc3))])
            center = (hh + ll) / 2
            diff = hlc3[k:] - center

            smoothed_diff = np.convolve(diff, np.ones(d) / d, mode='valid')
            smoothed_range = np.convolve(hh - ll, np.ones(d) / d, mode='valid')
            smi_raw = 100 * smoothed_diff / (smoothed_range + 1e-9)
            smi = np.convolve(smi_raw, np.ones(s) / s, mode='valid')

            if len(smi) > 0:
                smi_val = round(smi_raw[-1], 2)
                smi_signal_val = round(smi[-1], 2)
                print(f"[SMI] {symbol}: SMI={smi_val}, Signal={smi_signal_val}", flush=True)

        # === LR канал ===
        trend = None
        if len(candles) >= lr_len:
            closes_np = np.array(closes[-lr_len:])
            x = np.arange(len(closes_np))
            norm = (closes_np - closes_np.mean()) / closes_np.std()
            slope, _ = np.polyfit(x, norm, 1)
            angle = round(degrees(atan(slope)), 2)

            if angle > angle_up:
                trend = "up"
            elif angle < angle_down:
                trend = "down"
            else:
                trend = "flat"

            slope_real, intercept_real = np.polyfit(x, closes_np, 1)
            regression_line = slope_real * x + intercept_real
            std_dev = np.std(closes_np - regression_line)
            mid = round(regression_line[-1], 4)
            upper = round(mid + 2 * std_dev, 4)
            lower = round(mid - 2 * std_dev, 4)

            print(f"[LR] {symbol}: angle={angle}°, trend={trend}, mid={mid}, upper={upper}, lower={lower}", flush=True)

    except Exception as e:
        print(f"[ERROR] Ошибка в process_candle: {e}", flush=True)

# Слушает Redis канал и запускает расчёт индикаторов по завершённой свече
async def redis_listener():
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ohlcv_m5_complete")
    print("[Redis] Подписка на канал 'ohlcv_m5_complete'", flush=True)

    async for message in pubsub.listen():
        if message["type"] == "message":
            raw_data = message["data"]
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode()

            print(f"[DEBUG] raw_data repr: {repr(raw_data)}", flush=True)

            try:
                data = json.loads(raw_data)
            except Exception as e:
                print(f"[ERROR] JSON decode error: {e}", flush=True)
                continue

            symbol = data.get("symbol")
            timestamp_str = data.get("timestamp")

            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except Exception as e:
                print(f"[ERROR] timestamp parse error: {e}", flush=True)
                continue

            try:
                await process_candle(symbol, timestamp)
            except Exception as e:
                print(f"[ERROR] ВНУТРИ process_candle: {e}", flush=True)

# Запуск прослушивания Redis
if __name__ == "__main__":
    asyncio.run(redis_listener())
