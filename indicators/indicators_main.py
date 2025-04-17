# indicators_main.py — шаг 6.2: расчёт RSI и SMI (через EMA)

import asyncio
import json
import math
import os
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

# EMA расчёт
def ema(data, period):
    if len(data) < period:
        return None
    alpha = 2 / (period + 1)
    ema_values = [sum(data[:period]) / period]
    for price in data[period:]:
        ema_values.append((price - ema_values[-1]) * alpha + ema_values[-1])
    return ema_values[-1]

# Расчёт индикаторов
async def process_candle(symbol, timestamp):
    print(f"[DEBUG] ВХОД: process_candle(symbol={symbol}, timestamp={timestamp})", flush=True)

    try:
        result = session.execute(select(settings_table)).fetchall()
        if not result:
            print(f"[ERROR] Таблица indicator_settings пуста", flush=True)
            return

        # Построение словаря настроек
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

        rsi_period = settings.get("rsi", {}).get("period", 14)
        smi_k = settings.get("smi", {}).get("k", 13)
        smi_d = settings.get("smi", {}).get("d", 5)
        smi_s = settings.get("smi", {}).get("s", 3)

        lookback = max(
            rsi_period,
            smi_k + smi_d + smi_s,
            settings.get("lr", {}).get("length", 50)
        )

        candles = session.query(ohlcv_table) \
            .filter(ohlcv_table.c.symbol == symbol) \
            .filter(ohlcv_table.c.open_time <= timestamp) \
            .filter(ohlcv_table.c.complete == True) \
            .order_by(ohlcv_table.c.open_time.desc()) \
            .limit(lookback) \
            .all()

        candles = list(reversed(candles))
        print(f"[DEBUG] Загружено {len(candles)} свечей для {symbol}", flush=True)

        if len(candles) < lookback:
            print(f"[WARNING] Недостаточно данных для индикаторов ({len(candles)} < {lookback})", flush=True)
            return

        # === RSI ===
        closes = [c.close for c in candles]
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
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        midpoints = [(h + l) / 2 for h, l in zip(highs, lows)]
        diffs = [h - l for h, l in zip(highs, lows)]
        close_minus_mid = [c - m for c, m in zip(closes, midpoints)]

        cmd_ema = ema(close_minus_mid[-(smi_k + smi_d + smi_s):], smi_k)
        hl_ema = ema(diffs[-(smi_k + smi_d + smi_s):], smi_k)

        cmd_smoothed = ema([cmd_ema], smi_s) if cmd_ema is not None else None
        hl_smoothed = ema([hl_ema], smi_s) if hl_ema is not None else None

        if cmd_smoothed is not None and hl_smoothed and hl_smoothed != 0:
            smi = 100 * cmd_smoothed / hl_smoothed
            print(f"[DEBUG] SMI для {symbol} @ {timestamp}: {smi:.2f}", flush=True)
        else:
            print(f"[WARNING] SMI не рассчитан для {symbol}", flush=True)

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