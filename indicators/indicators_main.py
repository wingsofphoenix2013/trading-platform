# indicators/indicators_main.py — RSI + SMI + ATR + LR (linreg + 2σ)

print("🚀 INDICATORS WORKER STARTED", flush=True)

# === Импорты ===
import asyncio
import os
import asyncpg
import redis.asyncio as redis
import json
import pandas as pd
import numpy as np
from datetime import datetime
from math import atan, degrees

REDIS_CHANNEL_IN = 'ohlcv_m5_complete'
REDIS_CHANNEL_OUT = 'indicators_m5_live'

async def main():
    print("[INIT] Connecting to Redis...", flush=True)
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD"),
            db=0,
            decode_responses=True,
            ssl=True
        )
        await redis_client.ping()
        print("[OK] Connected to Redis", flush=True)

        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_CHANNEL_IN)
        print(f"[INIT] Subscribed to Redis channel: {REDIS_CHANNEL_IN}", flush=True)

    except Exception as e:
        print(f"[ERROR] Redis connection or subscription failed: {e}", flush=True)
        return

    print("[INIT] Connecting to PostgreSQL...", flush=True)
    try:
        pg_conn = await asyncpg.connect(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT", "5432"),
            database=os.getenv("PG_NAME")
        )
        print("[OK] Connected to PostgreSQL", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to connect PostgreSQL: {e}", flush=True)
        return

    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            data = json.loads(message['data'])
            symbol = data.get("symbol")
            ts_str = data.get("timestamp")
            print(f"[REDIS] Получено сообщение: symbol={symbol}, timestamp={ts_str}", flush=True)

            # === Загрузка 100 свечей ===
            query_candles = """
                SELECT open_time AS timestamp, open, high, low, close, volume
                FROM ohlcv_m5
                WHERE symbol = $1 AND complete = true
                ORDER BY open_time DESC
                LIMIT 100
            """
            rows = await pg_conn.fetch(query_candles, symbol)
            if not rows or len(rows) < 20:
                print(f"[SKIP] Недостаточно данных для {symbol} ({len(rows)} свечей)", flush=True)
                continue

            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp')
            print(f"[DATA] Загружено {len(df)} свечей для {symbol}", flush=True)

            # === Параметры ===
            query_settings = "SELECT indicator, param, value FROM indicator_settings"
            rows = await pg_conn.fetch(query_settings)
            settings = {}
            for row in rows:
                indicator = row['indicator']
                param = row['param']
                value = float(row['value'])
                if indicator not in settings:
                    settings[indicator] = {}
                settings[indicator][param] = value
            print(f"[DATA] Загружены параметры: {settings}", flush=True)

            # === RSI === [...]
            # === SMI === [...]
            # === ATR === [...]

            # === LR (линейный канал) ===
            try:
                lr_length = int(settings.get('lr', {}).get('length', 50))
                angle_up = settings.get('lr', {}).get('angle_up', 2)
                angle_down = settings.get('lr', {}).get('angle_down', -2)

                if len(df) < lr_length:
                    raise ValueError(f"Недостаточно данных для LR: нужно {lr_length}, есть {len(df)}")

                lr_df = df.tail(lr_length).copy()
                x = np.arange(lr_length)
                y = lr_df['close'].values

                # Линейная регрессия
                coef = np.polyfit(x, y, 1)
                slope = coef[0]
                intercept = coef[1]
                regression_line = slope * x + intercept

                # Угол наклона
                norm_slope = slope / np.mean(y)
                angle_deg = round(degrees(atan(norm_slope)), 2)

                # Тренд по порогам
                if angle_deg > angle_up:
                    trend = 'up'
                elif angle_deg < angle_down:
                    trend = 'down'
                else:
                    trend = 'flat'

                # Отклонение и границы
                std_dev = np.std(y - regression_line)
                lr_top = round(regression_line[-1] + 2 * std_dev, precision_digits)
                lr_bot = round(regression_line[-1] - 2 * std_dev, precision_digits)

                print(f"[LR] {symbol}: угол={angle_deg}°, тренд={trend}, верх={lr_top}, низ={lr_bot}", flush=True)

            except Exception as e:
                print(f"[ERROR] LR calculation failed for {symbol}: {e}", flush=True)
                continue

            # === UPDATE ===
            update_query = """
                UPDATE ohlcv_m5
                SET rsi = $1, smi = $2, smi_signal = $3, atr = $4,
                    lr_angle = $5, lr_trend = $6, lr_top = $7, lr_bottom = $8
                WHERE symbol = $9 AND open_time = $10
            """
            ts_dt = datetime.fromisoformat(ts_str)
            await pg_conn.execute(update_query, rsi_value, smi_value, smi_sig_value, atr_value,
                                   angle_deg, trend, lr_top, lr_bot, symbol, ts_dt)

            # === REDIS ===
            publish_data = {
                "symbol": symbol,
                "rsi": rsi_value,
                "smi": smi_value,
                "smi_signal": smi_sig_value,
                "atr": atr_value,
                "lr_angle": angle_deg,
                "lr_trend": trend,
                "lr_top": lr_top,
                "lr_bottom": lr_bot
            }
            await redis_client.publish(REDIS_CHANNEL_OUT, json.dumps(publish_data))
            print(f"[REDIS → {REDIS_CHANNEL_OUT}] Публикация индикаторов: {publish_data}", flush=True)

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке сообщения: {e}", flush=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Остановлено вручную", flush=True)
