import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime

# 🔸 Расчёт Stochastic Momentum Index и сигнальной EMA
async def process_smi(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        k = int(params.get("k", 13))
        d = int(params.get("d", 5))
        s = int(params.get("s", 3))  # сглаживание сигнальной линии

        if not all(col in candles.columns for col in ("high", "low", "close")):
            logging.warning(f"⚠️ Нет high/low/close в свечах {symbol} / {tf}")
            return

        high = candles["high"]
        low = candles["low"]
        close = candles["close"]

        if len(close) < k + d + s + 5:
            logging.warning(f"⚠️ Недостаточно данных для SMI {symbol} / {tf}")
            return

        hh = high.rolling(k).max()
        ll = low.rolling(k).min()
        mid = (hh + ll) / 2
        range_ = hh - ll
        rel = close - mid

        # Двойное EMA (Wilder-style)
        rel_ema = rel.ewm(alpha=1/d, adjust=False).mean().ewm(alpha=1/d, adjust=False).mean()
        range_ema = range_.ewm(alpha=1/d, adjust=False).mean().ewm(alpha=1/d, adjust=False).mean()

        smi = 200 * (rel_ema / range_ema)
        smi_value = round(float(smi.iloc[-1]), 2)

        # Сигнальная EMA
        smi_signal = smi.ewm(span=s, adjust=False).mean()
        signal_value = round(float(smi_signal.iloc[-1]), 2)

        # Ключи
        redis_key_main = f"{symbol}:{tf}:SMI:{k}_{d}"
        redis_key_signal = f"{symbol}:{tf}:SMI_SIGNAL:{k}_{d}_{s}"

        await redis.set(redis_key_main, smi_value)
        await redis.set(redis_key_signal, signal_value)

        open_dt = datetime.fromisoformat(open_time)
        param_main = f"smi{k}_{d}"
        param_signal = f"smi_signal{k}_{d}_{s}"

        async with db.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5),
                       ($1, $2, $3, $6, $7)
                ON CONFLICT DO NOTHING
                """,
                [(instance_id, symbol, open_dt, param_main, smi_value, param_signal, signal_value)]
            )

        logging.info(f"✅ SMI{k}_{d}_{s} для {symbol} / {tf} = {smi_value} / {signal_value}")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "SMI",
                        "params": json.dumps({"k": k, "d": d, "s": s}),
                        "calculated_at": open_time
                    }
                )
                logging.info(f"📤 Stream: SMI{k}_{d}_{s} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации SMI в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта SMI {symbol} / {tf}: {e}")