import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime
from debug_utils import debug_log

# 🔸 Расчёт линейной регрессии и запись в Redis + БД + Stream
async def process_lr(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        length = int(params.get("length", 50))
        if "close" not in candles.columns:
            logging.warning(f"⚠️ Нет close в свечах {symbol} / {tf}")
            return

        closes = candles["close"].astype(float)
        if len(closes) < length:
            logging.warning(f"⚠️ Недостаточно данных для расчёта LR {symbol} / {tf}")
            return

        closes = closes.tail(length).reset_index(drop=True)
        x = np.arange(length)

        # 🔹 A. Линейная регрессия по ценам
        coeffs_raw = np.polyfit(x, closes, deg=1)
        slope_raw = coeffs_raw[0]
        mid_raw = closes.mean()
        intercept_raw = mid_raw - slope_raw * (length // 2) + ((1 - (length % 2)) / 2) * slope_raw

        reg_line_raw = slope_raw * x + intercept_raw
        std_dev = np.sqrt(np.mean((closes - reg_line_raw) ** 2))

        upper_val = round(float(reg_line_raw[-1] + 2 * std_dev), precision_price)
        lower_val = round(float(reg_line_raw[-1] - 2 * std_dev), precision_price)
        mid_val   = round(float(reg_line_raw[-1]), precision_price)

        # 🔹 B. Расчёт угла по нормализованным данным
        base_price = closes.mean()
        norm = (closes - base_price) / base_price
        coeffs_norm = np.polyfit(x, norm, deg=1)
        slope_norm = coeffs_norm[0]
        angle_val = round(float(np.degrees(np.arctan(slope_norm))), 5)

        # Сохранение значений
        open_dt = datetime.fromisoformat(open_time)

        results = [
            ("lr_upper", upper_val),
            ("lr_lower", lower_val),
            ("lr_mid",   mid_val),
            ("lr_angle", angle_val)
        ]

        # Redis
        for param_name, value in results:
            redis_key = f"{symbol}:{tf}:LR:{param_name}"
            await redis.set(redis_key, value)

        # БД
        async with db.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO indicator_values_v2
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                """,
                [(instance_id, symbol, open_dt, param, val) for param, val in results]
            )

        debug_log(f"✅ LR{length} для {symbol} / {tf} рассчитан (angle={angle_val})")

        if stream_publish:
            try:
                await redis.xadd(
                    "indicators_ready_stream",
                    {
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": "LR",
                        "params": json.dumps({"length": str(length)}),
                        "calculated_at": open_time
                    }
                )
                debug_log(f"📤 Stream: LR{length} опубликован для {symbol} / {tf}")
            except Exception as e:
                logging.error(f"❌ Ошибка публикации LR в Redis Stream: {e}")

    except Exception as e:
        logging.error(f"❌ Ошибка расчёта LR {symbol} / {tf}: {e}")