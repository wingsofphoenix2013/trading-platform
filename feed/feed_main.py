# feed/main.py — подписка на потоки + Redis Pub/Sub + загрузка тикеров из БД
# 1. Импорт библиотек
import asyncio
import json
import os
from datetime import datetime
import websockets
import redis.asyncio as redis
import asyncpg

# 2. Подключение к Redis (Upstash) через переменные окружения
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

# 3. Подключение к PostgreSQL через переменные окружения

import psycopg2
from urllib.parse import urlparse

try:
    db_url = os.getenv("DATABASE_URL")
    pg_conn = psycopg2.connect(db_url)
    print("[PG] ✅ Подключение к PostgreSQL установлено", flush=True)
except Exception as e:
    print(f"[PG] ❌ Ошибка подключения к PostgreSQL: {e}", flush=True)
    pg_conn = None

# 4. Словарь активных тикеров
active_tickers = {}

# 5. Запускает WebSocket-потоки по тикеру

async def subscribe_ticker(symbol):
    if symbol in active_tickers:
        print(f"[INFO] {symbol} уже подписан", flush=True)
        return

    print(f"[INFO] Активируем подписку для {symbol}", flush=True)

    async def mark_price():
        url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice"
        async for ws in websockets.connect(url):
            try:
                async for message in ws:
                    data = json.loads(message)
                    price = data.get("p")
                    if price:
                        pass  # логи по mark price отключены
            except websockets.ConnectionClosed:
                print(f"[MARK PRICE] reconnecting: {symbol}", flush=True)
                continue

    async def m1_kline():
        url = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_1m"
        async for ws in websockets.connect(url):
            try:
                async for message in ws:
                    data = json.loads(message)
                    k = data.get("k", {})
                    if k and k.get("x"):
                        print(f"[M1 CANDLE] {datetime.utcnow()} - {symbol}: O:{k['o']} H:{k['h']} L:{k['l']} C:{k['c']}", flush=True)
                        save_m1_candle_with_diagnostics(pg_conn, data)
            except websockets.ConnectionClosed:
                print(f"[KLINE] reconnecting: {symbol}", flush=True)
                continue

    task1 = asyncio.create_task(mark_price())
    task2 = asyncio.create_task(m1_kline())
    active_tickers[symbol] = (task1, task2)

# 6. Слушает Redis канал и активирует новые тикеры по команде
async def redis_listener():
    pubsub = r.pubsub()
    await pubsub.subscribe("ticker_activation")
    print("[REDIS] Подписан на канал 'ticker_activation'", flush=True)

    async for message in pubsub.listen():
        if message["type"] == "message":
            try:
                data = json.loads(message["data"])
                if data.get("action") == "activate":
                    symbol = data.get("symbol", "").upper()
                    if symbol:
                        await subscribe_ticker(symbol)
            except Exception as e:
                print(f"[ERROR] Ошибка разбора сообщения: {e}", flush=True)


# 8. Запись M1-свечи в базу данных с диагностикой пропуска предыдущей

def save_m1_candle_with_diagnostics(conn, candle: dict):
    """
    Сохраняет минутную свечу в таблицу ohlcv_m1, проверяя наличие предыдущей свечи.
    Устанавливает флаг missing_previous, а также source и aggregated.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from datetime import datetime, timedelta

    try:
        symbol = candle["s"]
        kline = candle["k"]
        open_time_ms = kline["t"]
        open_time = datetime.utcfromtimestamp(open_time_ms / 1000).replace(second=0, microsecond=0)
        previous_time = open_time - timedelta(minutes=1)

        open_price = float(kline["o"])
        high_price = float(kline["h"])
        low_price = float(kline["l"])
        close_price = float(kline["c"])
        volume = float(kline["v"])
        inserted_at = datetime.utcnow()

        # Проверка наличия предыдущей свечи
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT 1 FROM ohlcv_m1 WHERE symbol = %s AND open_time = %s",
                (symbol, previous_time)
            )
            missing_previous = cur.fetchone() is None

        print(f"[M1] Вставка свечи {symbol} @ {open_time} | missing_previous = {missing_previous}", flush=True)

        # Вставка текущей свечи
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ohlcv_m1 (
                    symbol, open_time, open, high, low, close, volume,
                    inserted_at, missing_previous, aggregated, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, open_time) DO NOTHING
                """,
                (
                    symbol, open_time, open_price, high_price, low_price, close_price, volume,
                    inserted_at, missing_previous, False, 'stream'
                )
            )
            conn.commit()

        print(f"[M1] ✅ Успешно сохранено: {symbol} @ {open_time}", flush=True)

    except Exception as e:
        print(f"[M1] ❌ Ошибка при сохранении свечи: {e}", flush=True)
        
# X. Точка входа в модуль: запускает асинхронный главный цикл
async def main():
    print("🔥 FEED STARTED", flush=True)
    await listen_redis_channel()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
