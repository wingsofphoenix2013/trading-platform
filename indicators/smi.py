# smi.py
# Расчёт Stochastic Momentum Index (SMI) и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Безопасное округление значений
def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))

# Двойное EMA-сглаживание
def double_ema(series, length):
    return series.ewm(span=length, adjust=False).mean().ewm(span=length, adjust=False).mean()

# Основная функция расчёта SMI
async def process_smi(pg_pool, redis, symbol, tf, precision):
    # Получение параметров из indicator_settings
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT param, value
            FROM indicator_settings
            WHERE indicator = 'smi'
        """)
        settings = {r['param']: int(r['value']) for r in rows}
        k = settings.get('k', 13)
        d = settings.get('d', 5)
        s = settings.get('s', 3)

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

    # Расчёт SMI
    hh = df['high'].rolling(window=k).max()
    ll = df['low'].rolling(window=k).min()
    center = (hh + ll) / 2
    range_ = hh - ll
    rel = df['close'] - center

    smi_raw = 200 * (double_ema(rel, d) / double_ema(range_, d))
    smi_signal = smi_raw.ewm(span=s, adjust=False).mean()

    results = []
    for i in range(len(df)):
        if pd.isna(smi_raw[i]) or pd.isna(smi_signal[i]):
            continue
        ts = df.iloc[i]['open_time']
        results.append((symbol, tf, ts, 'SMI', 'smi', safe_round(smi_raw[i], precision)))
        results.append((symbol, tf, ts, 'SMI', 'smi_signal', safe_round(smi_signal[i], precision)))

    # Публикация последних значений в Redis
    if results:
        last = results[-2:]
        for r in last:
            redis_key = f"{symbol}:{tf}:{r[3]}:{r[4]}"
            await redis.set(redis_key, r[5])

    # Запись в БД
    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
            """,
            results
        )

    print(f"[SMI] Расчёт завершён для {symbol} / {tf}", flush=True)
