import logging
import pandas as pd
import json
from datetime import datetime
from ta.momentum import StochasticMomentumIndexIndicator

# 🔸 Расчёт SMI и сигнальной линии через ta-lib
async def process_smi(instance_id, symbol, tf, open_time, params, candles, redis, db, precision_price, stream_publish):
    try:
        # 🔹 Параметры SMI
        k = int(params.get("k", 13))
        d = int(params.get("d", 5))
        s = int(params.get("s", 3))  # сигнальная линия

        # 🔹 Приведение колонок к float (во избежание Decimal)
        high = candles["high"].astype(float)
        low = candles["low"].astype(float)
        close = candles["close"].astype(float)

        # 🔹 Проверка достаточности данных
        min_len = max(k, d * 2, s * 2) + 10
        if len(close) < min_len:
            logging.warning(f"⚠️ Недостаточно данных для SMI {symbol} / {tf}")
            return

        # 🔹 Создание индикатора
        indicator = StochasticMomentumIndexIndicator(
            close=close,
            high=high,
            low=low,
            window=k,
            smooth_window=d,
            smooth_window2=s
        )

        smi_value = round(float(indicator.stoch_momentum().iloc[-1]), 2)
        signal_value = round(float(indicator.stoch_momentum_signal().iloc[-1]), 2)

        # 🔹 Ключи Redis
        redis_key_main = f"{symbol}:{tf}:SMI:{k}_{d}"
        redis_key_signal = f"{symbol}:{tf}:SMI_SIGNAL:{k}_{d}_{s}"

        await redis.set(redis_key_main, smi_value)
        await redis.set(redis_key_signal, signal_value)

        # 🔹 Подготовка к записи
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