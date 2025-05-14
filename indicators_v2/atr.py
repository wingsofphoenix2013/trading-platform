import logging
import pandas as pd
import json
from datetime import datetime

# 🔸 Расчёт ATR + Median(30) и публикация в Redis + БД + Stream
async def process_atr(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        length = int(params.get("length", 14))

        if not all(col in candles.columns for col in ("high", "low", "close")):
            logging.warning(f"⚠️ Нет high/low/close в свечах {symbol} / {tf}")
            return

        high = candles["high"].astype(float)
        low = candles["low"].astype(float)
        close = candles["close"].astype(float)
        prev_close = close.shift(1)

        tr = pd.concat([
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)

        # ATR (Wilder-style)
        atr_series = tr.ewm(alpha=1/length, adjust=False).mean()
        atr_value = round(float(atr_series.iloc[-1]), precision_price)

        # 🔹 Median(30) по close
        if len(close) >= 30:
            median_30_val = round(float(close.tail(30).median()), precision_price)
            redis_key_median = f"{symbol}:{tf}:ATR:median_30"
            await redis.set(redis_key_median, median_30_val)
            debug_log(f"📊 Median(30) для {symbol} / {tf} = {median_30_val}")
        else:
            logging.warning(f"⚠️ Недостаточно данных для median(30) {symbol} / {tf}")

        # Основной ключ ATR
        redis_key = f"{symbol}:{tf}:ATR:{length}"
        await redis.set(redis_key, atr_value)

        open_dt = datetime.fromisoformat(open_time)
        param_name = f"atr{length}"

        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                instance_id, symbol, open_dt, param_name, atr_value
            )

        logging.info(f"✅ ATR{length} для {symbol} / {tf} = {atr_value}")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "ATR",
                        "params": json.dumps({"length": str(length)}),
                        "calculated_at": open_time
                    }
                )
                logging.info(f"📤 Stream: ATR{length} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации ATR в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта ATR {symbol} / {tf}: {e}")