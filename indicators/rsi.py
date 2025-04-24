# rsi.py
# Расчёт RSI по формуле Wilder (RMA) и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Безопасное округление

def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# Основная функция расчёта RSI
async def process_rsi(pg_pool, redis, symbol, tf, precision):
    # Получение параметра length
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT value FROM indicator_settings
            WHERE indicator = 'rsi' AND param = 'length'
        """)
        length = int(row['value']) if row else 14

    table_name = f"ohlcv_{tf.lower()}"
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, close
            FROM {table_name}
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
    df.reset_index(drop=True, inplace=True)
    df['close'] = df['close'].astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'])

    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()

    rsi_series = 100 - (100 / (1 + avg_gain / avg_loss))

    results = []
    for i in range(len(df)):
        if pd.isna(rsi_series[i]):
            continue
        ts = df.iloc[i]['open_time']
        results.append((symbol, tf, ts, 'RSI', 'rsi', safe_round(rsi_series[i], 2)))

    if results:
        last = results[-1]
        redis_key = f"{symbol}:{tf}:RSI:rsi"
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

    print(f"[RSI] Расчёт завершён для {symbol} / {tf}", flush=True)
