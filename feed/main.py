# feed/main.py — модуль сбора рыночных данных с Binance (WebSocket-only, логи упрощены)

print("🔥 MAIN.PY LAUNCHED", flush=True)

import asyncio
import json
import websockets
from datetime import datetime

# 🔧 Настройки тикера и WebSocket каналов
TICKER = "BTCUSDT"
MARK_PRICE_WS = f"wss://fstream.binance.com/ws/{TICKER.lower()}@markPrice"
KLINE_WS = f"wss://fstream.binance.com/ws/{TICKER.lower()}@kline_1m"

# 🔌 Подключение к WebSocket Binance (фьючерсы) — поток цен (без логов)
async def stream_mark_price():
    async for ws in websockets.connect(MARK_PRICE_WS):
        try:
            async for message in ws:
                data = json.loads(message)
                _ = data.get("p")  # цена используется, но не логируется
        except websockets.ConnectionClosed:
            print("[WebSocket] Disconnected. Reconnecting...", flush=True)
            continue

# 🔌 Подключение к WebSocket Binance (фьючерсы) — свечи M1
async def stream_kline_m1():
    print(f"[DEBUG] Connecting to WebSocket {KLINE_WS}", flush=True)
    async for ws in websockets.connect(KLINE_WS):
        try:
            print("[DEBUG] WebSocket connected — KLINE", flush=True)
            async for message in ws:
                data = json.loads(message)
                kline = data.get("k", {})
                if kline:
                    open_price = kline.get("o")
                    high = kline.get("h")
                    low = kline.get("l")
                    close = kline.get("c")
                    is_closed = kline.get("x")
                    if is_closed:
                        print(f"[M1 CANDLE] {datetime.utcnow()} - O:{open_price} H:{high} L:{low} C:{close}", flush=True)
        except websockets.ConnectionClosed:
            print("[WebSocket] Kline disconnected. Reconnecting...", flush=True)
            continue

# 🧠 Главный цикл: два потока WebSocket — mark price и свечи M1
async def main():
    print("[MAIN] Starting data feed module (WebSocket only)...", flush=True)
    try:
        await asyncio.gather(
            stream_mark_price(),
            stream_kline_m1()
        )
    except Exception as e:
        print(f"[FATAL ERROR] {e}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
