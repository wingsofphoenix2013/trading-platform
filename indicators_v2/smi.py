import logging
import pandas as pd
import json
from datetime import datetime

# 🔸 Расчёт SMI по альтернативной формуле (Surjith v2.1)
async def process_smi(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        k = int(params.get("k", 10))   # Percent K Length (range window)
        d = int(params.get("d", 3))    # Percent D Length (EMA smoothing)
        s = int(params.get("s", 10))   # EMA Signal Length
        smooth = int(params.get("smooth", 5))  # Smoothing Period (SMA)

        if not all(col in candles.columns for col in ("high", "low", "close")):
            logging.warning(f"⚠️ Нет high/low/close в свечах {symbol} / {tf}")
            return

        high = candles["high"].astype(float)
        low = candles["low"].astype(float)
        close = candles["close"].astype(float)

        if len(close) < k + d + s + smooth + 5:
            logging.warning(f"⚠️ Недостаточно данных для расчёта SMI {symbol} / {tf}")
            return

        hh = high.rolling(window=k).max()
        ll = low.rolling(window=k).min()
        diff = hh - ll
        rdiff = close - (hh + ll) / 2

        avgrel = rdiff.ewm(span=d, adjust=False).mean()
        avgdiff = diff.ewm(span=d, adjust=False).mean()

        smi_raw = 100 * (avgrel / (avgdiff / 2))
        smi_smoothed = smi_raw.rolling(window=smooth).mean()
        signal_line = smi_smoothed.ewm(span=s, adjust=False).mean()

        smi_val = round(float(smi_smoothed.iloc[-1]), 2)
        signal_val = round(float(signal_line.iloc[-1]), 2)

        redis_key_main = f"{symbol}:{tf}:SMI_ALT:{k}_{d}_{smooth}"
        redis_key_signal = f"{symbol}:{tf}:SMI_ALT_SIGNAL:{k}_{d}_{smooth}_{s}"

        await redis.set(redis_key_main, smi_val)
        await redis.set(redis_key_signal, signal_val)

        open_dt = datetime.fromisoformat(open_time)
        param_main = f"smi_alt{k}_{d}_{smooth}"
        param_signal = f"smi_alt_signal{k}_{d}_{smooth}_{s}"

        async with db.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5),
                       ($1, $2, $3, $6, $7)
                ON CONFLICT DO NOTHING
                """,
                [(instance_id, symbol, open_dt, param_main, smi_val, param_signal, signal_val)]
            )

        logging.info(f"✅ SMI_ALT {k}_{d}_{smooth}_{s} для {symbol} / {tf} = {smi_val} / {signal_val}")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "SMI_ALT",
                        "params": json.dumps({"k": k, "d": d, "smooth": smooth, "s": s}),
                        "calculated_at": open_time
                    }
                )
                logging.info(f"📤 Stream: SMI_ALT {k}_{d}_{smooth}_{s} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации SMI_ALT в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта SMI_ALT {symbol} / {tf}: {e}")