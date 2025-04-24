# atr.py
# Расчёт ATR и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Безопасное округление значений
def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# Основная функция расчёта ATR
async def process_atr(pg_pool, redis, symbol, tf, precision):
    # Получение параметров из indicator_settings
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT value FROM indicator_settings
            WHERE indicator = 'atr' AND param = 'length'
        """)
        length = int(row['value']) if row else 14

    table_name = f"ohlcv_{tf.lower()}"
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, high, low, close
            FROM {table_name}
            WHERE symbol = $1
            ORDER BY open_time DESC
            LIMIT 250
            """,
            symbol
        )

    if not rows:
        return

    df = pd.DataFrame(rows, columns=['open_time', 'high', 'low', 'close'])
    df = df[::-1]
    df.reset_index(drop=True, inplace=True)
    df[['high', 'low', 'close']] = df[['high', 'low', 'close']].astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'])

    # Расчёт TR
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs()
    ], axis=1).max(axis=1)

    # ATR через EMA
    atr_series = tr.ewm(alpha=1/length, adjust=False).mean()

    results = []
    for i in range(len(df)):
        if pd.isna(atr_series[i]):
            continue
        ts = df.iloc[i]['open_time']
        results.append((symbol, tf, ts, 'ATR', 'atr', safe_round(atr_series[i], precision)))

    if results:
        last = results[-1]
        redis_key = f"{symbol}:{tf}:ATR:atr"
        await redis.set(redis_key, last[5])

    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            """,
            results
        )

    print(f"[ATR] Расчёт завершён для {symbol} / {tf}", flush=True)
