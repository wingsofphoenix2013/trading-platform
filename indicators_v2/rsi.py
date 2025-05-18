import logging
import pandas as pd
import json
from datetime import datetime
from debug_utils import debug_log

# 🔸 Очистка старых значений (оставляем только 100 последних)
async def cleanup_old_values(db, instance_id, symbol, param_name):
    try:
        async with db.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM indicator_values_v2
                WHERE ctid IN (
                    SELECT ctid FROM (
                        SELECT ctid,
                               ROW_NUMBER() OVER (
                                   PARTITION BY instance_id, symbol, param_name
                                   ORDER BY open_time DESC
                               ) AS rownum
                        FROM indicator_values_v2
                        WHERE instance_id = $1 AND symbol = $2 AND param_name = $3
                    ) sub
                    WHERE sub.rownum > 300
                )
                """,
                instance_id, symbol, param_name
            )
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке RSI {param_name} для {symbol}: {e}")
        
# 🔸 Расчёт RSI (Wilder's Smoothing) и сохранение в Redis + БД + Stream
async def process_rsi(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        length = int(params.get("length", 14))

        if "close" not in candles.columns:
            logging.warning(f"⚠️ Нет close в свечах {symbol} / {tf}")
            return

        closes = candles["close"].astype(float)
        if len(closes) < length + 1:
            logging.warning(f"⚠️ Недостаточно данных для RSI {symbol} / {tf}")
            return

        delta = closes.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        rsi_value = round(float(rsi.iloc[-1]), 2)

        redis_key = f"{symbol}:{tf}:RSI:{length}"
        await redis.set(redis_key, rsi_value)

        open_dt = datetime.fromisoformat(open_time)
        param_name = f"rsi{length}"

        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                instance_id, symbol, open_dt, param_name, rsi_value
            )
            
        # 🔹 Очистка лишних значений
        await cleanup_old_values(db, instance_id, symbol, param_name)
        
        debug_log(f"✅ RSI{length} для {symbol} / {tf} = {rsi_value}")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "RSI",
                        "params": json.dumps({"length": str(length)}),
                        "calculated_at": open_time
                    }
                )
                debug_log(f"📤 Stream: RSI{length} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации RSI в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта RSI {symbol} / {tf}: {e}")