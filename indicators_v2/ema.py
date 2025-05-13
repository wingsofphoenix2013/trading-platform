import logging
import pandas as pd
from datetime import datetime

# 🔸 Очистка старых значений EMA (оставляем только 100 последних)
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
                    WHERE sub.rownum > 100
                )
                """,
                instance_id, symbol, param_name
            )
        # debug_log(f"🧹 Очистка значений EMA {param_name} для {symbol} завершена")
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке EMA {param_name} для {symbol}: {e}")

# 🔸 Расчёт EMA и сохранение в Redis + БД
async def process_ema(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price):
    try:
        length = int(params.get("length", 9))

        if "close" not in candles.columns:
            logging.warning(f"⚠️ Нет close в свечах {symbol} / {tf}")
            return

        closes = candles["close"]
        ema_series = closes.ewm(span=length, adjust=False).mean()
        ema_value = round(float(ema_series.iloc[-1]), precision_price)

        redis_key = f"{symbol}:{tf}:EMA:{length}"
        await redis.set(redis_key, ema_value)

        open_dt = datetime.fromisoformat(open_time)
        param_name = f"ema{length}"

        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                instance_id, symbol, open_dt, param_name, ema_value
            )

        await cleanup_old_values(db, instance_id, symbol, param_name)

        logging.info(f"✅ EMA{length} для {symbol} / {tf} = {ema_value}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта EMA {symbol} / {tf}: {e}")