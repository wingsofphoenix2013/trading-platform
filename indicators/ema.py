# ema.py
# Расчёт EMA 50/100/200 и запись в БД + Redis

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# 1. Безопасное округление значений
def safe_round(value, digits):
    return float(Decimal(value).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_HALF_UP))


# 2. Основная функция расчёта EMA
async def process_ema(pg_pool, redis, symbol, tf, precision):
    print(f"[EMA] Начинаем расчёт EMA для {symbol} / {tf}", flush=True)

    table_name = f"ohlcv_{tf.lower()}"
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, close FROM {table_name}
            WHERE symbol = $1
            ORDER BY open_time ASC
            LIMIT 300
            """,
            symbol
        )

    if not rows:
        print(f"[EMA] Нет данных для {symbol} / {tf}", flush=True)
        return

    df = pd.DataFrame(rows, columns=['open_time', 'close'])
    df['close'] = df['close'].astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'])

    # Вычисление EMA
    results = []
    for length in [50, 100, 200]:
        df[f'ema_{length}'] = df['close'].ewm(span=length, adjust=False).mean()

        for index, row in df.iterrows():
            value = safe_round(row[f'ema_{length}'], precision)
            results.append((row['open_time'], f"ema{length}", value))

            # Публикация в Redis — только последнее значение
            if index == df.index[-1]:
                redis_key = f"{symbol}:{tf}:EMA:{length}"
                await redis.set(redis_key, value)
                print(f"[Redis] {redis_key} → {value}", flush=True)

    # Запись в БД
    async with pg_pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO indicator_values (symbol, timeframe, open_time, indicator, param_name, value)
            VALUES ($1, $2, $3, 'EMA', $4, $5)
            ON CONFLICT DO NOTHING
            """,
            [(symbol, tf, ts, param, val) for ts, param, val in results]
        )

    print(f"[EMA] Записано EMA для {symbol} / {tf}", flush=True)
