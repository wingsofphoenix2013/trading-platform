import logging
import pandas as pd

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

        async with db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                instance_id, symbol, open_time, f"ema{length}", ema_value
            )

        logging.info(f"✅ EMA{length} для {symbol} / {tf} = {ema_value}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта EMA {symbol} / {tf}: {e}")