# signal_main.py — обработчик сигналов (background worker)

import asyncio
import asyncpg
import redis.asyncio as redis
import json
import os
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

# --- Конфигурация окружения ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# --- Глобальные переменные (в оперативной памяти) ---
active_tickers = set()
active_signals = {}  # phrase: {id, direction}
strategy_bindings = {}  # ticker_symbol -> [strategy_id, ...]

# --- Подключение к БД ---
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# --- Загрузка всех активных тикеров ---
async def load_active_tickers():
    conn = await get_db()
    rows = await conn.fetch("SELECT symbol FROM tickers WHERE status = 'enabled'")
    await conn.close()
    return set(row["symbol"] for row in rows)

# --- Загрузка всех активных сигналов ---
async def load_active_signals():
    conn = await get_db()
    rows = await conn.fetch("SELECT id, long_phrase, short_phrase, long_exit_phrase, short_exit_phrase FROM signals WHERE enabled = true")
    await conn.close()
    phrases = {}
    for row in rows:
        for direction_field in ["long_phrase", "short_phrase", "long_exit_phrase", "short_exit_phrase"]:
            phrase = row[direction_field]
            if phrase:
                phrases[phrase] = {
                    "id": row["id"],
                    "direction": direction_field.replace("_phrase", "")
                }
    return phrases

# --- Загрузка связей стратегий и тикеров ---
async def load_strategy_bindings():
    conn = await get_db()
    rows = await conn.fetch("SELECT strategy_id, t.symbol FROM strategy_tickers st JOIN tickers t ON st.ticker_id = t.id WHERE st.enabled = true")
    await conn.close()
    bindings = {}
    for row in rows:
        symbol = row["symbol"]
        strategy_id = row["strategy_id"]
        bindings.setdefault(symbol, []).append(strategy_id)
    return bindings

# --- Обработка сообщений из Redis ---
async def redis_listener():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, ssl=True)
    pubsub = r.pubsub()
    await pubsub.subscribe("ticker_activation", "signal_activation")

    print("[redis] Подписка на каналы: ticker_activation, signal_activation", flush=True)

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
        except Exception as e:
            print(f"[redis] Ошибка парсинга JSON: {e}", flush=True)
            continue

        channel = message["channel"].decode()
        print(f"[redis] Сообщение в {channel}: {data}", flush=True)

        if channel == "ticker_activation":
            symbol = data.get("symbol")
            action = data.get("action")
            if symbol and action == "activate":
                active_tickers.add(symbol)
                print(f"[tickers] Активирован тикер: {symbol}", flush=True)
            elif symbol and action == "deactivate":
                active_tickers.discard(symbol)
                print(f"[tickers] Деактивирован тикер: {symbol}", flush=True)

        elif channel == "signal_activation":
            signal_id = data.get("id")
            enabled = data.get("enabled")
            if signal_id is not None:
                active_signals.clear()
                active_signals.update(await load_active_signals())
                print(f"[signals] Обновлён список сигналов (triggered by id={signal_id}, enabled={enabled})", flush=True)

# --- FastAPI приложение для приёма сигналов ---
app = FastAPI()

@app.post("/webhook", response_class=PlainTextResponse)
async def webhook(request: Request):
    try:
        body = await request.body()
        message = body.decode("utf-8").strip()
    except Exception:
        return PlainTextResponse("Malformed request", status_code=400)

    if " " not in message:
        status = "error"
        signal_id = None
        ticker = "unknown"
        direction = "unknown"
    else:
        phrase, ticker = message.split(" ", 1)
        phrase = phrase.strip()
        ticker = ticker.strip().upper()

        signal_info = active_signals.get(phrase)
        if not signal_info:
            status = "ignored"
            signal_id = None
            direction = "unknown"
        elif ticker not in active_tickers:
            status = "ignored"
            signal_id = signal_info["id"]
            direction = signal_info["direction"]
        else:
            status = "new"
            signal_id = signal_info["id"]
            direction = signal_info["direction"]

    conn = await get_db()
    await conn.execute("""
        INSERT INTO signal_logs (signal_id, ticker_symbol, direction, source, raw_message, received_at, status)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6)
    """, signal_id, ticker, direction, "tradingview", message, status)
    await conn.close()
    
    print(f"[webhook] message='{message}' | signal_id={signal_id} | ticker={ticker} | direction={direction} | status={status}", flush=True)
    return PlainTextResponse(f"Logged with status: {status}")

# --- Выполняется при старте FastAPI ---
@app.on_event("startup")
async def startup_event():
    global active_tickers, active_signals, strategy_bindings

    print("[signal_worker] Запуск...", flush=True)
    active_tickers = await load_active_tickers()
    active_signals = await load_active_signals()
    strategy_bindings = await load_strategy_bindings()

    print(f"[init] Тикеры: {len(active_tickers)} | Сигналы: {len(active_signals)} | Стратегии: {len(strategy_bindings)}", flush=True)
    print("[main] Начинаем слушать Redis...", flush=True)

    asyncio.create_task(redis_listener())

# --- Точка входа ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)