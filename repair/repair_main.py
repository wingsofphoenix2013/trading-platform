import os
import asyncpg
import asyncio
from datetime import datetime

# Проверка на пропуски M1-свечей по тикеру
def detect_gaps(rows):
    gaps = []
    for i in range(1, len(rows)):
        prev = rows[i - 1]['open_time']
        curr = rows[i]['open_time']
        delta = (curr - prev).total_seconds()
        if delta > 60:
            gaps.append((prev, curr, int(delta)))
    return gaps


async def check_symbol(conn, symbol):
    print(f"[CHECK] {symbol}: проверка на пропуски...", flush=True)

    rows = await conn.fetch("""
        SELECT open_time
        FROM ohlcv_m1
        WHERE symbol = $1
        ORDER BY open_time ASC
    """, symbol)

    if not rows:
        print(f"[WARN] Нет данных по {symbol}", flush=True)
        return

    gaps = detect_gaps(rows)
    if gaps:
        print(f"[GAPS] {symbol}: найдено {len(gaps)} разрывов", flush=True)
        for prev, curr, delta in gaps:
            print(f"  ⛔ {symbol} | {prev} → {curr} = {delta} сек", flush=True)
    else:
        print(f"[OK] {symbol}: без пропусков", flush=True)


async def main():
    db_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(dsn=db_url)

    symbols = await conn.fetch("SELECT DISTINCT symbol FROM ohlcv_m1")
    for record in symbols:
        symbol = record['symbol']
        await check_symbol(conn, symbol)

    await conn.close()

if __name__ == '__main__':
    asyncio.run(main())
