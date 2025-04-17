# indicators/indicators_main.py — RSI + SMI + ATR (Wilder RMA)

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

            # === Параметры индикаторов ===
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

            # === RSI ===
            rsi_period = int(settings.get('rsi', {}).get('period', 14))
            delta = df['close'].diff()
            gain = np.where(delta > 0, delta, 0)
            loss = np.where(delta < 0, -delta, 0)
            avg_gain = pd.Series(gain).rolling(window=rsi_period).mean()
            avg_loss = pd.Series(loss).rolling(window=rsi_period).mean()
            rs = avg_gain / avg_loss
            rsi_series = 100 - (100 / (1 + rs))
            rsi_value = round(rsi_series.iloc[-1], 2)
            print(f"[RSI] {symbol}: {rsi_value}", flush=True)

            # === SMI ===
            try:
                smi_k = int(settings.get('smi', {}).get('k', 13))
                smi_d = int(settings.get('smi', {}).get('d', 5))
                smi_s = int(settings.get('smi', {}).get('s', 3))

                hl2 = (df['high'] + df['low']) / 2
                min_low = hl2.rolling(window=smi_k).min()
                max_high = hl2.rolling(window=smi_k).max()
                smi_raw = hl2 - (max_high + min_low) / 2
                smi_div = (max_high - min_low) / 2
                smi_base = 100 * smi_raw / smi_div.replace(0, np.nan)

                smi_ema = smi_base.ewm(span=smi_s).mean()
                smi_signal = smi_ema.ewm(span=smi_d).mean()

                smi_value = round(smi_ema.iloc[-1], 2)
                smi_sig_value = round(smi_signal.iloc[-1], 2)

                print(f"[SMI] {symbol}: {smi_value}, сигнальная: {smi_sig_value}", flush=True)

            except Exception as e:
                print(f"[ERROR] SMI calculation failed for {symbol}: {e}", flush=True)
                continue

            # === ATR (Wilder RMA) ===
            try:
                atr_period = int(settings.get('atr', {}).get('period', 14))
                high = df['high']
                low = df['low']
                close = df['close']
                prev_close = close.shift(1)

                tr1 = high - low
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()

                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr_series = tr.ewm(alpha=1/atr_period, adjust=False).mean()

                query_precision = "SELECT precision_price FROM tickers WHERE symbol = $1"
                precision_row = await pg_conn.fetchrow(query_precision, symbol)
                precision_digits = int(precision_row['precision_price']) if precision_row else 2

                atr_value = round(atr_series.iloc[-1], precision_digits)
                print(f"[ATR] {symbol}: {atr_value} (точность: {precision_digits})", flush=True)

            except Exception as e:
                print(f"[ERROR] ATR calculation failed for {symbol}: {e}", flush=True)
                continue

            # === Обновление ohlcv_m5 ===
            update_query = """
                UPDATE ohlcv_m5
                SET rsi = $1, smi = $2, smi_signal = $3, atr = $4
                WHERE symbol = $5 AND open_time = $6
            """
            ts_dt = datetime.fromisoformat(ts_str)
            await pg_conn.execute(update_query, rsi_value, smi_value, smi_sig_value, atr_value, symbol, ts_dt)
            print(f"[DB] RSI + SMI + ATR записаны в ohlcv_m5 для {symbol} @ {ts_str}", flush=True)

            # === Публикация в Redis ===
            publish_data = {
                "symbol": symbol,
                "rsi": rsi_value,
                "smi": smi_value,
                "smi_signal": smi_sig_value,
                "atr": atr_value
            }
            await redis_client.publish(REDIS_CHANNEL_OUT, json.dumps(publish_data))
            print(f"[REDIS → {REDIS_CHANNEL_OUT}] Публикация индикаторов: {publish_data}", flush=True)

        except Exception as e:
            print(f"[ERROR] Ошибка при обработке сообщения: {e}", flush=True)

# === Точка входа ===
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[STOP] Остановлено вручную", flush=True)
