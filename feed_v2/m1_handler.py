# m1_handler.py
# Получение свечей M1 по WebSocket для активных тикеров и активация через Redis

# 0. Импорты
import asyncio
import websockets
import json
from datetime import datetime, timedelta

# Глобальный словарь активных потоков по тикерам
active_tickers = {}


# 1. Запрос активных тикеров из базы данных
async def get_enabled_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled'")
        return [row["symbol"] for row in rows]


# 2. Публикация в Redis с защитой от падения
async def safe_publish(redis, channel, message):
    try:
        await redis.publish(channel, json.dumps(message))
        print(f"[REDIS] Published to {channel}: {message}", flush=True)
    except Exception as e:
        print(f"[ERROR] Redis publish failed: {e}", flush=True)


# 3. Сохранение M1-свечи в базу данных
async def save_m1_candle(pg_pool, redis, symbol, kline):
    open_time = datetime.utcfromtimestamp(kline["t"] / 1000)
    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ohlcv2_m1 (symbol, open_time, open, high, low, close, volume, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol, open_time) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source = EXCLUDED.source
            """,
            symbol,
            open_time,
            kline["o"],
            kline["h"],
            kline["l"],
            kline["c"],
            kline["v"],
            "stream"
        )

    await safe_publish(redis, "ohlcv_m1_ready", {
        "action": "m1_ready",
        "symbol": symbol,
        "open_time": open_time.isoformat()
    })


# 4. Подключение к WebSocket Binance и логирование закрытых свечей
async def subscribe_m1_kline(symbol, pg_pool, redis):
    if symbol in active_tickers:
        print(f"[M1] Уже подписан: {symbol}", flush=True)
        return

    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_1m"
    print(f"[M1] Подключение к WebSocket для {symbol}", flush=True)

    async def stream():
        async for ws in websockets.connect(url):
            try:
                async for message in ws:
                    data = json.loads(message)
                    kline = data.get("k", {})
                    if kline.get("x"):  # закрытая свеча
                        ts = datetime.utcfromtimestamp(kline["t"] / 1000)
                        print(f"[M1 CANDLE] {symbol} {ts} O:{kline['o']} H:{kline['h']} L:{kline['l']} C:{kline['c']}", flush=True)
                        await save_m1_candle(pg_pool, redis, symbol, kline)
            except websockets.ConnectionClosed:
                print(f"[M1] Переподключение: {symbol}", flush=True)
                continue
            except Exception as e:
                print(f"[ERROR] Ошибка WebSocket для {symbol}: {e}", flush=True)
                await asyncio.sleep(5)

    task = asyncio.create_task(stream())
    active_tickers[symbol] = task


# 5. Запуск всех текущих тикеров + Redis-слушатель + фоновая проверка
async def start_all_m1_streams(redis, pg_pool):
    symbols = await get_enabled_tickers(pg_pool)
    print(f"[M1] Тикеры из БД: {symbols}", flush=True)
    for symbol in symbols:
        await subscribe_m1_kline(symbol, pg_pool, redis)

    asyncio.create_task(redis_listener(redis, pg_pool))
    asyncio.create_task(watch_new_tickers(pg_pool, redis))
    asyncio.create_task(check_missing_m1(pg_pool))


# 6. Устойчивый Redis listener: восстанавливает соединение при обрыве
async def redis_listener(redis, pg_pool):
    while True:
        try:
            pubsub = redis.pubsub()
            await pubsub.subscribe("ticker_activation")
            print("[REDIS] Подписка на ticker_activation активна", flush=True)

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    data = json.loads(message["data"])
                    if data.get("action") == "activate":
                        symbol = data.get("symbol", "").upper()
                        if symbol:
                            await subscribe_m1_kline(symbol, pg_pool, redis)
                except Exception as e:
                    print(f"[ERROR] Ошибка разбора сообщения Redis: {e}", flush=True)

        except Exception as e:
            print(f"[ERROR] Redis listener упал, переподключение через 5 сек: {e}", flush=True)
            await asyncio.sleep(5)


# 7. Периодическая проверка на новые тикеры из БД (на случай пропуска Redis-сообщения)
async def watch_new_tickers(pg_pool, redis):
    while True:
        try:
            symbols = await get_enabled_tickers(pg_pool)
            for symbol in symbols:
                if symbol not in active_tickers:
                    print(f"[M1] Новый тикер из БД: {symbol}", flush=True)
                    await subscribe_m1_kline(symbol, pg_pool, redis)
        except Exception as e:
            print(f"[ERROR] Ошибка при проверке тикеров из БД: {e}", flush=True)

        await asyncio.sleep(300)  # каждые 5 минут


# 8. Контроль пропущенных свечей и запись в missing_m1_log
async def check_missing_m1(pg_pool):
    while True:
        try:
            target_time = datetime.utcnow().replace(second=0, microsecond=0) - timedelta(minutes=1)
            async with pg_pool.acquire() as conn:
                for symbol in active_tickers:
                    row = await conn.fetchrow(
                        "SELECT 1 FROM ohlcv2_m1 WHERE symbol = $1 AND open_time = $2",
                        symbol,
                        target_time
                    )
                    if row is None:
                        await conn.execute(
                            """
                            INSERT INTO missing_m1_log (symbol, open_time)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                            """,
                            symbol,
                            target_time
                        )
                        print(f"[MISSING] Не найдена свеча M1: {symbol} @ {target_time}", flush=True)
        except Exception as e:
            print(f"[ERROR] Ошибка в check_missing_m1: {e}", flush=True)

        await asyncio.sleep(60)  # раз в минуту
