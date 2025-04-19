# feed_main.py — подписка на потоки + Redis Pub/Sub + загрузка тикеров из БД

print("🔥 FEED STARTED", flush=True)

# Импорт библиотек
import asyncio
import json
import os
from datetime import datetime
import websockets
import redis.asyncio as redis
import asyncpg

# Подключение к Redis (Upstash) через переменные окружения
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

# Подключение к PostgreSQL через переменные окружения
async def get_enabled_tickers():
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled'")
        await conn.close()
        return [row["symbol"] for row in rows]
    except Exception as e:
        print(f"[ERROR] DB connection failed: {e}", flush=True)
        return []

# Запись свечи M1 в базу данных
async def save_m1_candle(symbol, kline):
    db_url = os.getenv("DATABASE_URL")
    try:
        conn = await asyncpg.connect(dsn=db_url)
        await conn.execute("""
            INSERT INTO ohlcv_m1 (symbol, open_time, open, high, low, close, volume)
            VALUES ($1, to_timestamp(CAST($2 AS DOUBLE PRECISION) / 1000), $3, $4, $5, $6, $7)
        """,
            symbol,
            kline["t"], kline["o"], kline["h"],
            kline["l"], kline["c"], kline["v"]
        )
        await conn.close()
    except Exception as e:
        print(f"[ERROR] Ошибка при записи M1-свечи: {e}", flush=True)
        
# 🧩 Агрегация M5-свечей из M1 с установкой флага complete и публикацией в Redis
async def aggregate_m5_candles():
    db_url = os.getenv("DATABASE_URL")

    while True:
        now = datetime.utcnow()
        if now.minute % 5 == 0 and now.second < 5:
            try:
                conn = await asyncpg.connect(dsn=db_url)
                for symbol in active_tickers.keys():
                    rows = await conn.fetch("""
                        SELECT * FROM ohlcv_m1
                        WHERE symbol = $1
                        ORDER BY open_time DESC
                        LIMIT 5
                    """, symbol)

                    if len(rows) == 5:
                        rows = sorted(rows, key=lambda r: r["open_time"])
                        open_time = rows[0]["open_time"]
                        open = rows[0]["open"]
                        high = max(r["high"] for r in rows)
                        low = min(r["low"] for r in rows)
                        close = rows[-1]["close"]
                        volume = sum(r["volume"] for r in rows)

                        # Вставка свечи M5 с флагом complete = TRUE
                        await conn.execute("""
                            INSERT INTO ohlcv_m5 (symbol, open_time, open, high, low, close, volume, complete)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
                        """, symbol, open_time, open, high, low, close, volume)

                        # Публикация сообщения о готовности свечи в Redis
                        message = {
                            "symbol": symbol,
                            "timestamp": open_time.isoformat()
                        }
                        await r.publish("ohlcv_m5_complete", json.dumps(message))

                await conn.close()

            except Exception as e:
                print(f"[ERROR] Агрегация M5: {e}", flush=True)

        # Периодический запуск цикла с шагом в 5 секунд
        await asyncio.sleep(5)
# Агрегация M15-свечей из M1
async def aggregate_m15_candles():
    db_url = os.getenv("DATABASE_URL")

    while True:
        now = datetime.utcnow()
        if now.minute % 15 == 0 and now.second < 5:
            try:
                conn = await asyncpg.connect(dsn=db_url)
                for symbol in active_tickers.keys():
                    rows = await conn.fetch("""
                        SELECT * FROM ohlcv_m1
                        WHERE symbol = $1
                        ORDER BY open_time DESC
                        LIMIT 15
                    """, symbol)

                    if len(rows) == 15:
                        rows = sorted(rows, key=lambda r: r["open_time"])
                        open_time = rows[0]["open_time"]
                        open = rows[0]["open"]
                        high = max(r["high"] for r in rows)
                        low = min(r["low"] for r in rows)
                        close = rows[-1]["close"]
                        volume = sum(r["volume"] for r in rows)

                        await conn.execute("""
                            INSERT INTO ohlcv_m15 (symbol, open_time, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """, symbol, open_time, open, high, low, close, volume)

                await conn.close()
            except Exception as e:
                print(f"[ERROR] Агрегация M15: {e}", flush=True)

        await asyncio.sleep(5)
# Словарь активных тикеров
active_tickers = {}

# Запускает WebSocket-потоки по тикеру
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
                        pass # логи по mark price отключены
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
                        await save_m1_candle(symbol, k)
            except websockets.ConnectionClosed:
                print(f"[KLINE] reconnecting: {symbol}", flush=True)
                continue

    task1 = asyncio.create_task(mark_price())
    task2 = asyncio.create_task(m1_kline())
    active_tickers[symbol] = (task1, task2)

# Слушает Redis канал и активирует новые тикеры по команде
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

# Главный запуск: активируем все тикеры из БД + слушаем Redis
async def main():
    print("[MAIN] Feed module running", flush=True)

    symbols = await get_enabled_tickers()
    print(f"[MAIN] Тикеров из БД для активации: {symbols}", flush=True)
    for symbol in symbols:
        await subscribe_ticker(symbol)

    # запуск фоновой задачи агрегации M5
    asyncio.create_task(aggregate_m5_candles())
    
    # запуск фоновой задачи агрегации М15
    asyncio.create_task(aggregate_m15_candles())

    # Слушаем Redis для динамической активации
    await redis_listener()

# Точка входа в модуль: запускает асинхронный главный цикл
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())