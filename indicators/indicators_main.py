# indicators_main.py — шаг 4: загрузка параметров из нормализованной таблицы indicator_settings

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

# Шаг 4: Загрузка нормализованных параметров индикаторов
async def process_candle(symbol, timestamp):
    print(f"[DEBUG] ВХОД: process_candle(symbol={symbol}, timestamp={timestamp})", flush=True)

    try:
        result = session.execute(select(settings_table)).fetchall()

        if not result:
            print(f"[ERROR] Таблица indicator_settings пуста", flush=True)
            return

        # Построение словаря: {'rsi': {'period': 14}, 'smi': {'k': ..., 'd': ...}, ...}
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

            # Преобразуем числовые значения
            try:
                value = float(value)
                if value.is_integer():
                    value = int(value)
            except:
                pass  # оставить как строку, если не число

            settings[indicator][param] = value

        print(f"[DEBUG] Построенные настройки: {settings}", flush=True)

    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке или разборе настроек: {e}", flush=True)

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