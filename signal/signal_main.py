# signal_main.py — обработчик сигналов (background worker)

import asyncio
import asyncpg
import redis.asyncio as redis
import json
import os
from datetime import datetime

# --- Конфигурация окружения ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# --- Глобальные переменные (в оперативной памяти) ---
active_tickers = set()
active_signals = {}  # signal_phrase: {id, direction, ...}
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

    print("[redis] Подписка на каналы: ticker_activation, signal_activation")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
        except Exception as e:
            print(f"[redis] Ошибка парсинга JSON: {e}")
            continue

        channel = message["channel"].decode()
        print(f"[redis] Сообщение в {channel}: {data}")

        if channel == "ticker_activation":
            symbol = data.get("symbol")
            action = data.get("action")
            if symbol and action == "activate":
                active_tickers.add(symbol)
                print(f"[tickers] Активирован тикер: {symbol}")
            elif symbol and action == "deactivate":
                active_tickers.discard(symbol)
                print(f"[tickers] Деактивирован тикер: {symbol}")

        elif channel == "signal_activation":
            signal_id = data.get("id")
            enabled = data.get("enabled")
            if signal_id is not None:
                active_signals.clear()
                active_signals.update(await load_active_signals())
                print(f"[signals] Обновлён список сигналов (triggered by id={signal_id}, enabled={enabled})")
# --- Главная точка входа ---
async def main():
    global active_tickers, active_signals, strategy_bindings

    print("[signal_worker] Запуск...", flush=True)
    active_tickers = await load_active_tickers()
    active_signals = await load_active_signals()
    strategy_bindings = await load_strategy_bindings()

    print(f"[init] Тикеры: {len(active_tickers)} | Сигналы: {len(active_signals)} | Стратегии: {len(strategy_bindings)}", flush=True)
    print("[main] Начинаем слушать Redis...", flush=True)

    await redis_listener()

if __name__ == "__main__":
    print("[entrypoint] Стартуем asyncio loop...")
    asyncio.run(main())
    print("[exit] worker завершился (main() закончился)", flush=True)