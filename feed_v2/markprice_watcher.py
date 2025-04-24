# markprice_watcher.py
# Получение mark price по каждому тикеру и обновление Redis

# 0. Импорты
import asyncio
import websockets
import json
import time

# 1. Подписка на WebSocket Binance и обновление Redis
async def watch_markprice(symbol, redis):
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice"
    print(f"[MARK] Подключение к потоку markPrice для {symbol}", flush=True)

    async def stream():
        last_update = 0
        async for ws in websockets.connect(url):
            try:
                async for message in ws:
                    data = json.loads(message)
                    price = data.get("p")
                    if price:
                        now = time.time()
                        if now - last_update >= 1:
                            await redis.set(f"price:{symbol}", float(price))
                            last_update = now
            except websockets.ConnectionClosed:
                print(f"[MARK] Переподключение: {symbol}", flush=True)
                continue
            except Exception as e:
                print(f"[ERROR] Ошибка markPrice для {symbol}: {e}", flush=True)
                await asyncio.sleep(5)

    asyncio.create_task(stream())


# 2. Запуск по всем тикерам
async def start_markprice_watchers(symbols, redis):
    for symbol in symbols:
        await watch_markprice(symbol, redis)
