# ema.py
# Расчёт EMA 50/100/200 и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# 1. Безопасное округление значений
def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# 2. Расчёт EMA вручную через SMA-старт
def manual_ema(prices, length):
    alpha = 2 / (length + 1)
    ema = [sum(prices[:length]) / length]  # стартовое значение = SMA
    for price in prices[length:]:
        ema.append(alpha * price + (1 - alpha) * ema[-1])
    return [None] * (length - 1) + ema

# 3. Основная функция расчёта EMA
async def process_ema(pg_pool, redis, symbol, tf, precision):
    table_name = f"ohlcv_{tf.lower()}"
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, close FROM {table_name}
            WHERE symbol = $1
            ORDER BY open_time DESC
            LIMIT 250
            """,
            symbol
        )

    if not rows:
        return

    df = pd.DataFrame(rows, columns=['open_time', 'close'])
    df = df[::-1]
    df['close'] = df['close'].astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'])

    results = []
    prices = df['close'].tolist()

    for length in [50, 100, 200]:
        ema_series = manual_ema(prices, length)
        df[f'ema_{length}'] = ema_series

        for index, row in df.iterrows():
            raw_val = row[f'ema_{length}']
            if raw_val is None:
                continue
            value = safe_round(raw_val, precision)
            results.append((row['open_time'], f"ema{length}", value))

            if index == df.index[-1]:
                redis_key = f"{symbol}:{tf}:EMA:{length}"
                await redis.set(redis_key, value)

    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, 'EMA', $4, $5)
            ON CONFLICT DO NOTHING
            """,
            [(symbol, tf, ts, param, val) for ts, param, val in results]
        )

    print(f"[EMA] Расчёт завершён для {symbol} / {tf}", flush=True)
