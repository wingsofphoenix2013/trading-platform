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
        logging.error(f"❌ Ошибка при очистке MFI {param_name} для {symbol}: {e}")
        
# 🔸 Расчёт Money Flow Index (MFI) и публикация
async def process_mfi(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        length = int(params.get("length", 14))

        required_cols = ("high", "low", "close", "volume")
        if not all(col in candles.columns for col in required_cols):
            logging.warning(f"⚠️ Недостаточно колонок для MFI {symbol} / {tf}")
            return

        high = candles["high"].astype(float)
        low = candles["low"].astype(float)
        close = candles["close"].astype(float)
        volume = candles["volume"].astype(float)

        if len(close) < length + 1:
            logging.warning(f"⚠️ Недостаточно данных для MFI {symbol} / {tf}")
            return

        tp = (high + low + close) / 3  # typical price
        rmf = tp * volume              # raw money flow
        delta_tp = tp.diff()

        pos_flow = rmf.where(delta_tp > 0, 0.0)
        neg_flow = rmf.where(delta_tp < 0, 0.0)

        pos_sum = pos_flow.rolling(length).sum()
        neg_sum = neg_flow.rolling(length).sum()

        mfr = pos_sum / neg_sum.replace(0, 1e-6)  # защита от деления на 0
        mfi_series = 100 - (100 / (1 + mfr))
        mfi_value = round(float(mfi_series.iloc[-1]), 2)

        # Redis ключ
        redis_key = f"{symbol}:{tf}:MFI:{length}"
        await redis.set(redis_key, mfi_value)

        open_dt = datetime.fromisoformat(open_time)
        param_name = f"mfi{length}"

        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                instance_id, symbol, open_dt, param_name, mfi_value
            )

        await cleanup_old_values(db, instance_id, symbol, param_name)
        
        debug_log(f"✅ MFI{length} для {symbol} / {tf} = {mfi_value}")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "MFI",
                        "params": json.dumps({"length": str(length)}),
                        "calculated_at": open_time
                    }
                )
                logging.info(f"📤 Stream: MFI{length} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации MFI в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта MFI {symbol} / {tf}: {e}")