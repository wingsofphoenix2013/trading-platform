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

# --- Обработка входящего сигнала из Redis ---
async def handle_incoming_signal(data):
    message = data.get("message")
    source = data.get("source", "unknown")

    if not message or " " not in message:
        print(f"[signal] Некорректный формат: '{message}'", flush=True)
        return

    phrase, ticker = message.split(" ", 1)
    phrase = phrase.strip()
    ticker = ticker.strip().upper()

    signal_info = active_signals.get(phrase)
    if not signal_info:
        signal_id = None
        direction = "unknown"
        status = "ignored"
    elif ticker not in active_tickers:
        signal_id = signal_info["id"]
        direction = signal_info["direction"]
        status = "ignored"
    else:
        signal_id = signal_info["id"]
        direction = signal_info["direction"]
        status = "new"

    # --- Сохраняем сигнал в signal_logs и получаем log_id
    conn = await get_db()
    log_id = await conn.fetchval("""
        INSERT INTO signal_logs (signal_id, ticker_symbol, direction, source, raw_message, received_at, status)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6)
        RETURNING id
    """, signal_id, ticker, direction, source, message, status)

    print(f"[signal] Получен сигнал: '{message}' → status={status}, direction={direction}, ticker={ticker}", flush=True)

    # --- Если статус new, находим стратегии и записываем log_entries
    if status == "new":
        strategies = await conn.fetch("""
            SELECT s.id, s.use_all_tickers
            FROM strategies s
            JOIN strategy_signals ss ON ss.strategy_id = s.id
            WHERE s.enabled = true AND ss.signal_id = $1 AND ss.role = 'action'
        """, signal_id)

        if not strategies:
            print(f"[signal] Нет стратегий, реагирующих на сигнал {signal_id}", flush=True)
            await conn.close()
            return

        # --- Redis клиент
        redis_conn = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True
        )

        for s in strategies:
            strategy_id = s["id"]
            allowed = False

            if s["use_all_tickers"]:
                allowed = True
            else:
                allowed = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM strategy_tickers st
                        JOIN tickers t ON st.ticker_id = t.id
                        WHERE st.enabled = true AND st.strategy_id = $1 AND t.symbol = $2
                    )
                """, strategy_id, ticker)

            if not allowed:
                print(f"[signal] Стратегия {strategy_id} не разрешает тикер {ticker}, пропуск", flush=True)
                continue

            # --- Запись действия стратегии по сигналу
            entry_id = await conn.fetchval("""
                INSERT INTO signal_log_entries (log_id, strategy_id, status, logged_at)
                VALUES ($1, $2, 'new', NOW())
                RETURNING id
            """, log_id, strategy_id)

            # --- Публикуем log_id (а не entry_id) для запуска стратегии
            await redis_conn.publish("signal_logs_ready", str(log_id))
            print(f"[signal] Стратегия {strategy_id} добавлена в очередь, log_entry_id={entry_id}", flush=True)

    await conn.close()
    
# --- Обработка сообщений из Redis ---
async def redis_listener():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, ssl=True)
    pubsub = r.pubsub()
    await pubsub.subscribe("ticker_activation", "signal_activation", "incoming_signals")

    print("[redis] Подписка на каналы: ticker_activation, signal_activation, incoming_signals", flush=True)

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

        elif channel == "incoming_signals":
            await handle_incoming_signal(data)

# --- FastAPI приложение (для отладки или расширения) ---
app = FastAPI()

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
