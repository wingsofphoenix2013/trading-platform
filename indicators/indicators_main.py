# indicators_main.py — реактивный расчёт индикаторов по сигналу из Redis

import asyncio
import json
import math
import os
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, select, func
from sqlalchemy.orm import sessionmaker
import redis.asyncio as redis
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к БД
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

# Получение таблиц
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

# EMA-расчёт
def ema(data, period):
    if len(data) < period:
        return None
    alpha = 2 / (period + 1)
    ema_values = [sum(data[:period]) / period]
    for price in data[period:]:
        ema_values.append((price - ema_values[-1]) * alpha + ema_values[-1])
    return ema_values[-1]

# Основная функция расчёта по тикеру и времени
async def process_candle(symbol, timestamp):
    try:
        # Получение настроек
        settings_row = session.execute(
            select(settings_table).where(settings_table.c.symbol == symbol).limit(1)
        ).fetchone()

        lr_length = settings_row.lr_length if settings_row and settings_row.lr_length else 50
        angle_up_threshold = settings_row.angle_up_threshold if settings_row and settings_row.angle_up_threshold is not None else 2.0
        angle_down_threshold = settings_row.angle_down_threshold if settings_row and settings_row.angle_down_threshold is not None else -2.0
        rsi_period = settings_row.rsi_period if settings_row and settings_row.rsi_period else 14
        smi_k = settings_row.smi_k if settings_row and settings_row.smi_k else 13
        smi_d = settings_row.smi_d if settings_row and settings_row.smi_d else 5
        smi_smooth = settings_row.smi_smooth if settings_row and settings_row.smi_smooth else 3

        lookback = max(lr_length, rsi_period, smi_k + smi_d + smi_smooth)

        candles = session.query(ohlcv_table) \
            .filter(ohlcv_table.c.symbol == symbol) \
            .filter(ohlcv_table.c.timestamp <= timestamp) \
            .filter(ohlcv_table.c.complete == True) \
            .order_by(ohlcv_table.c.timestamp.desc()) \
            .limit(lookback) \
            .all()

        candles = list(reversed(candles))

        if len(candles) < lookback:
            logging.warning(f"Недостаточно данных для расчёта: {symbol} @ {timestamp}")
            return

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]

        # === Линейный канал ===
        lr_closes = closes[-lr_length:]
        x = list(range(len(lr_closes)))
        n = len(lr_closes)
        x_mean = sum(x) / n
        y_mean = sum(lr_closes) / n
        numerator = sum((x[i] - x_mean) * (lr_closes[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0
        angle_rad = math.atan(slope)
        angle_deg = angle_rad * 180 / math.pi

        trend = 'up' if angle_deg > angle_up_threshold else 'down' if angle_deg < angle_down_threshold else 'flat'
        regression_line = [y_mean + slope * (i - x_mean) for i in x]
        upper = max(lr_closes[i] - regression_line[i] for i in range(n))
        lower = min(lr_closes[i] - regression_line[i] for i in range(n))
        lr_upper = lr_closes[-1] + upper
        lr_lower = lr_closes[-1] + lower

        # === RSI ===
        gains = [max(closes[i+1] - closes[i], 0) for i in range(-rsi_period - 1, -1)]
        losses = [abs(min(closes[i+1] - closes[i], 0)) for i in range(-rsi_period - 1, -1)]
        avg_gain = sum(gains) / rsi_period
        avg_loss = sum(losses) / rsi_period
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs)) if avg_loss != 0 else 100

        # === SMI ===
        midpoints = [(highs[i] + lows[i]) / 2 for i in range(len(closes))]
        high_low_diff = [highs[i] - lows[i] for i in range(len(closes))]
        close_mid_diff = [closes[i] - midpoints[i] for i in range(len(closes))]
        hl_ema = ema(high_low_diff[-smi_k:], smi_k)
        cmd_ema = ema(close_mid_diff[-smi_k:], smi_k)
        hl_ema_smoothed = ema([hl_ema], smi_smooth) if hl_ema else None
        cmd_ema_smoothed = ema([cmd_ema], smi_smooth) if cmd_ema else None
        smi = (cmd_ema_smoothed / hl_ema_smoothed * 100) if hl_ema_smoothed and hl_ema_smoothed != 0 else None

        # Получение precision
        precision_result = session.execute(
            select(ohlcv_table.c.precision_price).where(ohlcv_table.c.symbol == symbol).limit(1)
        ).fetchone()
        precision = precision_result[0] if precision_result else 4

        # Обновление строки в базе
        session.execute(ohlcv_table.update()
            .where(ohlcv_table.c.symbol == symbol)
            .where(ohlcv_table.c.timestamp == timestamp)
            .values(
                lr_upper=round(lr_upper, precision),
                lr_lower=round(lr_lower, precision),
                lr_angle=round(angle_deg, 2),
                trend=trend,
                rsi=round(rsi, 2),
                smi=round(smi, 2) if smi else None
            )
        )
        session.commit()

        # Публикация индикаторов в Redis
        redis_msg = {
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "trend": trend,
            "rsi": round(rsi, 2),
            "smi": round(smi, 2) if smi else None
        }
        await redis_client.publish("indicators_updates", json.dumps(redis_msg))

        logging.info(f"🔔 Индикаторы обновлены: {symbol} @ {timestamp} → trend={trend}, rsi={rsi:.2f}, smi={redis_msg['smi']}")

    except Exception as e:
        logging.error(f"Ошибка при расчёте индикаторов: {e}")
        session.rollback()
# Слушает Redis канал и запускает расчёт индикаторов по завершённой свече
async def redis_listener():
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ohlcv_m5_complete")
    print("[Redis] Подписка на канал 'ohlcv_m5_complete'", flush=True)

    async for message in pubsub.listen():
        if message["type"] == "message":
            try:
                raw_data = message["data"]

                # Отладка: исходное сообщение
                print(f"[DEBUG] raw_data repr: {repr(raw_data)}", flush=True)
                print(f"[DEBUG] raw_data type: {type(raw_data)}", flush=True)

                # Декодирование байтов
                if isinstance(raw_data, bytes):
                    raw_data = raw_data.decode()

                # Попытка парсинга JSON
                try:
                    data = json.loads(raw_data)
                except json.JSONDecodeError as e:
                    print(f"[ERROR] JSON decode error: {e} | raw: {repr(raw_data)}", flush=True)
                    continue

                # Отладка: что распарсилось
                print(f"[DEBUG] parsed data: {repr(data)}", flush=True)
                print(f"[DEBUG] parsed data type: {type(data)}", flush=True)

                # Проверка ключей
                symbol = data.get("symbol")
                timestamp_str = data.get("timestamp")

                if not symbol or not timestamp_str:
                    print(f"[ERROR] Отсутствуют ключи 'symbol' или 'timestamp': {data}", flush=True)
                    continue

                # Преобразование времени
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError as e:
                    print(f"[ERROR] Неверный формат времени: {timestamp_str} | {e}", flush=True)
                    continue

                # Отдельный блок отладки вызова process_candle
                try:
                    await process_candle(symbol, timestamp)
                except Exception as e:
                    print(f"[ERROR] Ошибка в process_candle: {e}", flush=True)

            except Exception as e:
                print(f"[ERROR] Общая ошибка обработки сообщения из Redis: {e}", flush=True)
                
# Точка входа
if __name__ == "__main__":
    asyncio.run(redis_listener())
