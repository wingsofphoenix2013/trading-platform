# lr.py
# Расчёт линейного регрессионного канала и запись в БД + Redis

import numpy as np
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Округление

def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# Основная функция
async def process_lr(pg_pool, redis, symbol, tf, precision):
    # Чтение параметров из indicator_settings
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT param, value FROM indicator_settings WHERE indicator = 'lr'
        """)
        settings = {r['param']: float(r['value']) for r in rows}
        length = int(settings.get('length', 50))
        angle_up = settings.get('angle_up', 2.0)
        angle_down = settings.get('angle_down', -2.0)

    table_name = f"ohlcv2_{tf.lower()}"
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, close
            FROM {table_name}
            WHERE symbol = $1
            ORDER BY open_time DESC
            LIMIT {length + 20}
            """,
            symbol
        )

    if not rows or len(rows) < length:
        return

    df = pd.DataFrame(rows, columns=['open_time', 'close'])
    df = df[::-1]
    df.reset_index(drop=True, inplace=True)
    df['close'] = df['close'].astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'])

    df = df.tail(length)
    closes = df['close'].values
    base_price = np.mean(closes)
    norm = (closes - base_price) / base_price

    x = np.arange(len(norm))
    coeffs = np.polyfit(x, norm, deg=1)
    slope = coeffs[0]
    intercept = coeffs[1]

    angle = np.degrees(np.arctan(slope))

    if angle > angle_up:
        trend = 'up'
    elif angle < angle_down:
        trend = 'down'
    else:
        trend = 'flat'

    reg_line = slope * x + intercept
    std_dev = np.std(norm)
    upper = reg_line + 2 * std_dev
    lower = reg_line - 2 * std_dev

    latest_ts = df.iloc[-1]['open_time']
    mid = reg_line[-1]
    upper_val = safe_round(base_price * (1 + upper[-1]), precision)
    lower_val = safe_round(base_price * (1 + lower[-1]), precision)
    mid_val = safe_round(base_price * (1 + mid), precision)
    angle_val = safe_round(angle, 5)

    results = [
        (symbol, tf, latest_ts, 'LR', 'lr_upper', upper_val),
        (symbol, tf, latest_ts, 'LR', 'lr_lower', lower_val),
        (symbol, tf, latest_ts, 'LR', 'lr_mid', mid_val),
        (symbol, tf, latest_ts, 'LR', 'lr_angle', angle_val),
        (symbol, tf, latest_ts, 'LR', 'lr_trend', 1 if trend == 'up' else -1 if trend == 'down' else 0),
    ]

    for r in results:
        redis_key = f"{symbol}:{tf}:{r[3]}:{r[4]}"
        await redis.set(redis_key, str(r[5]))

    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            """,
            results
        )

    print(f"[LR] Расчёт завершён для {symbol} / {tf}", flush=True)
