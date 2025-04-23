import os
import asyncpg
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
import time
import sys

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

# Определение всех затронутых интервалов
def collect_affected_intervals(inserted_times):
    m5_set = set()
    m15_set = set()
    for ts in inserted_times:
        ts = ts.replace(second=0, microsecond=0)
        m5 = ts.replace(minute=(ts.minute // 5) * 5)
        m15 = ts.replace(minute=(ts.minute // 15) * 15)
        m5_set.add(m5)
        m15_set.add(m15)
    return sorted(m5_set), sorted(m15_set)

# Запрос недостающих свечей у Binance
async def fetch_klines(symbol, start_time, end_time):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1000
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"[ERROR] Binance API для {symbol}: {resp.status}", flush=True)
                return []
            data = await resp.json()
            return data

# Вставка свечей в базу + возврат всех open_time
async def insert_klines(conn, symbol, klines):
    inserted = 0
    inserted_times = []
    for k in klines:
        open_time = datetime.fromtimestamp(k[0] / 1000)
        try:
            await conn.execute("""
                INSERT INTO ohlcv_m1 (symbol, open_time, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, open_time) DO NOTHING
            """, symbol, open_time, k[1], k[2], k[3], k[4], k[5])
            inserted += 1
            inserted_times.append(open_time)
        except Exception as e:
            print(f"[ERROR] Вставка свечи {symbol} @ {open_time}: {e}", flush=True)
    print(f"[DB] {symbol}: вставлено {inserted} свечей", flush=True)
    return inserted_times

# Пересчёт M5
async def reaggregate_m5(conn, symbol, m5_open):
    rows = await conn.fetch("""
        SELECT * FROM ohlcv_m1
        WHERE symbol = $1 AND open_time >= $2 AND open_time < $3
        ORDER BY open_time ASC
    """, symbol, m5_open, m5_open + timedelta(minutes=5))
    if len(rows) != 5:
        return
    await conn.execute("DELETE FROM ohlcv_m5 WHERE symbol = $1 AND open_time = $2", symbol, m5_open)
    await conn.execute("""
        INSERT INTO ohlcv_m5 (symbol, open_time, open, high, low, close, volume, complete)
        VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
    """, symbol, m5_open,
        rows[0]['open'],
        max(r['high'] for r in rows),
        min(r['low'] for r in rows),
        rows[-1]['close'],
        sum(r['volume'] for r in rows))
    print(f"[M5] Пересчитано {symbol} @ {m5_open}", flush=True)

# Пересчёт M15
async def reaggregate_m15(conn, symbol, m15_open):
    rows = await conn.fetch("""
        SELECT * FROM ohlcv_m1
        WHERE symbol = $1 AND open_time >= $2 AND open_time < $3
        ORDER BY open_time ASC
    """, symbol, m15_open, m15_open + timedelta(minutes=15))
    if len(rows) != 15:
        return
    await conn.execute("DELETE FROM ohlcv_m15 WHERE symbol = $1 AND open_time = $2", symbol, m15_open)
    await conn.execute("""
        INSERT INTO ohlcv_m15 (symbol, open_time, open, high, low, close, volume, complete)
        VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
    """, symbol, m15_open,
        rows[0]['open'],
        max(r['high'] for r in rows),
        min(r['low'] for r in rows),
        rows[-1]['close'],
        sum(r['volume'] for r in rows))
    print(f"[M15] Пересчитано {symbol} @ {m15_open}", flush=True)

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

    # Предварительно: вывести сообщение о возможности повреждённых M5/M15
    print(f"[INFO] {symbol}: ранее могли быть записаны неверные M5/M15. Требуется пересчёт.", flush=True)

    gaps = detect_gaps(rows)
    if not gaps:
        print(f"[OK] {symbol}: без пропусков", flush=True)
        return

    print(f"[GAPS] {symbol}: найдено {len(gaps)} разрывов", flush=True)
    all_inserted = []
    for prev, curr, delta in gaps:
        print(f"  ⛔ {symbol} | {prev} → {curr} = {delta} сек", flush=True)
        print(f"[API] Запрос свечей {symbol} @ {prev + timedelta(minutes=1)} → {curr}", flush=True)
        klines = await fetch_klines(symbol, prev + timedelta(minutes=1), curr)
        inserted = await insert_klines(conn, symbol, klines)
        all_inserted.extend(inserted)

    # Вычисляем все затронутые интервалы и пересчитываем агрегаты
    m5_list, m15_list = collect_affected_intervals(all_inserted)
    for ts in m5_list:
        await reaggregate_m5(conn, symbol, ts)
    for ts in m15_list:
        await reaggregate_m15(conn, symbol, ts)

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
    time.sleep(5)
    sys.exit(0)