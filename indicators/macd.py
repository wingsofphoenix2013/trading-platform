# macd.py
# Расчёт MACD и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Безопасное округление

def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# Основная функция расчёта MACD
async def process_macd(pg_pool, redis, symbol, tf, precision):
    # Получение параметров из indicator_settings
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT param, value FROM indicator_settings WHERE indicator = 'macd'
        """)
        settings = {r['param']: int(r['value']) for r in rows}
        fast = settings.get('fast', 12)
        slow = settings.get('slow', 26)
        signal = settings.get('signal', 9)

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

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()

    results = []
    for i in range(len(df)):
        if pd.isna(macd_line[i]) or pd.isna(signal_line[i]):
            continue
        ts = df.iloc[i]['open_time']
        results.append((symbol, tf, ts, 'MACD', 'macd', safe_round(macd_line[i], precision)))
        results.append((symbol, tf, ts, 'MACD', 'macd_signal', safe_round(signal_line[i], precision)))

    if results:
        last = results[-2:]
        for r in last:
            redis_key = f"{symbol}:{tf}:{r[3]}:{r[4]}"
            await redis.set(redis_key, r[5])

    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            """,
            results
        )

    print(f"[MACD] Расчёт завершён для {symbol} / {tf}", flush=True)
