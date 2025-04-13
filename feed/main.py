# feed/main.py — модуль сбора рыночных данных с Binance

import asyncio
import json
import aiohttp
import websockets
from datetime import datetime

# 🔧 Настройки тикера и конечных точек Binance
TICKER = "BTCUSDT"
BASE_URL = "https://fapi.binance.com"
WS_URL = f"wss://fstream.binance.com/ws/{TICKER.lower()}@markPrice"
KLINES_URL = f"{BASE_URL}/fapi/v1/klines"

# 🧩 Получение минутных свечей M1 через REST
async def fetch_m1_klines():
    async with aiohttp.ClientSession() as session:
        params = {
            "symbol": TICKER,
            "interval": "1m",
            "limit": 1
        }
        async with session.get(KLINES_URL, params=params) as resp:
            data = await resp.json()
            candle = data[0]  # [time, open, high, low, close, volume, ...]
            print(f"[M1 CANDLE] {datetime.utcnow()} - O:{candle[1]} H:{candle[2]} L:{candle[3]} C:{candle[4]}")

# 🔌 Подключение к WebSocket Binance (поток цен)
async def stream_mark_price():
    async for ws in websockets.connect(WS_URL):
        try:
            async for message in ws:
                data = json.loads(message)
                price = data.get("p")
                if price:
                    print(f"[MARK PRICE] {datetime.utcnow()} - {TICKER}: {price}")
        except websockets.ConnectionClosed:
            print("[WebSocket] Disconnected. Reconnecting...")
            continue

# 🧠 Главный цикл: параллельно WebSocket и M1-опрос
async def main():
    await asyncio.gather(
        stream_mark_price(),
        poll_m1_candles()
    )

# ⏱️ Цикл вызова свечей каждую минуту
async def poll_m1_candles():
    while True:
        await fetch_m1_klines()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
