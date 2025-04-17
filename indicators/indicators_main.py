# indicators_main.py — шаг 6.4.1: восстановление расчёта линейного канала с нормализацией угла

import asyncio
import json
import math
import os
import numpy as np
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

        lookback = max(
            settings.get("rsi", {}).get("period", 14),
            settings.get("smi", {}).get("k", 13) + settings.get("smi", {}).get("d", 5) + settings.get("smi", {}).get("s", 3),
            lr_len
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

        closes = np.array([float(c.close) for c in candles])

        # === Линейный канал с нормализацией угла ===
        if len(closes) >= lr_len:
            y = closes[-lr_len:]
            x = np.arange(len(y))
            slope, intercept = np.polyfit(x, y, 1)

            # нормализация угла
            slope_normalized = slope * 1000 / y[-1]  # масштабирование
            angle_rad = math.atan(slope_normalized)
            angle_deg = angle_rad * 180 / math.pi

            regression = slope * x + intercept
            diffs = y - regression
            upper = y[-1] + max(diffs)
            lower = y[-1] + min(diffs)

            if angle_deg > angle_up:
                trend = "up"
            elif angle_deg < angle_down:
                trend = "down"
            else:
                trend = "flat"

            print(f"[LR] {symbol}: угол={angle_deg:.2f}°, тренд={trend}, верх={upper:.4f}, низ={lower:.4f}", flush=True)
        else:
            print(f"[WARNING] Недостаточно данных для LR ({len(closes)} < {lr_len})", flush=True)

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