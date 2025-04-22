# strategies_main.py — координатор стратегий

import asyncio
import asyncpg
import redis.asyncio as redis
import os
import json
from datetime import datetime

# --- Блок импорта стратегий ---
import vilarso_m5_flex
import vilarso_m5_fulltp
import lx_m5_strict
import vl_m5_strict

# --- Конфигурация окружения ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# --- Глобальные переменные ---
active_strategies = []
strategy_tickers_map = {}  # strategy_id → set of ticker_ids

# --- Подключение к БД ---
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# --- Загрузка стратегий ---
async def load_strategies():
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT id, name, enabled, use_all_tickers
        FROM strategies
        WHERE enabled = true
    """)
    await conn.close()
    return [dict(r) for r in rows]

# --- Загрузка активных тикеров по стратегиям ---
async def load_strategy_tickers():
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT strategy_id, ticker_id
        FROM strategy_tickers
        WHERE enabled = true
    """)
    await conn.close()
    mapping = {}
    for r in rows:
        mapping.setdefault(r["strategy_id"], set()).add(r["ticker_id"])
    return mapping

# --- Обработка сигнала: вызов нужной стратегии ---
async def handle_signal(signal_log_id: int):
    try:
        print(f"[strategies_main] Обработка signal_log_id={signal_log_id}", flush=True)
        conn = await get_db()

        row = await conn.fetchrow("""
            SELECT s.name
            FROM signal_log_entries sl
            JOIN strategies s ON s.id = sl.strategy_id
            WHERE sl.log_id = $1
        """, signal_log_id)

        await conn.close()

        if not row:
            print(f"[strategies_main] Стратегия для log_id={signal_log_id} не найдена", flush=True)
            return

        strategy_name = row["name"]

        if strategy_name == "vilarso_m5_flex":
            await vilarso_m5_flex.process_signal(signal_log_id)

        elif strategy_name == "lx_m5_strict":
            await lx_m5_strict.process_signal(signal_log_id)
            
        elif strategy_name == "vl_m5_strict":
            await vl_m5_strict.process_signal(signal_log_id)
            
        elif strategy_name == "vilarso_m5_fulltp":
            await vilarso_m5_fulltp.process_signal(signal_log_id)    
            
        else:
            print(f"[strategies_main] Стратегия '{strategy_name}' пока не поддерживается", flush=True)

    except Exception as e:
        print(f"[strategies_main] Ошибка при обработке сигнала: {e}", flush=True)

# --- Периодическая проверка на случай потери Redis-сообщений ---
async def periodic_refresh():
    global active_strategies, strategy_tickers_map
    while True:
        await asyncio.sleep(300)  # каждые 5 минут
        print("[refresh] Контрольная перезагрузка стратегий и связей из БД", flush=True)
        active_strategies = await load_strategies()
        strategy_tickers_map = await load_strategy_tickers()

# --- Подписка на Redis ---
async def redis_listener():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, ssl=True)
    pubsub = r.pubsub()
    await pubsub.subscribe("signal_logs_ready", "strategy_activation")
    print("[redis] Подписка на каналы: signal_logs_ready, strategy_activation", flush=True)

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            channel = message["channel"].decode()
            data = json.loads(message["data"])

            if channel == "signal_logs_ready":
                signal_log_id = int(data)
                await handle_signal(signal_log_id)

            elif channel == "strategy_activation":
                print(f"[redis] Перезагрузка стратегий по событию strategy_activation", flush=True)
                active_strategies = await load_strategies()
                strategy_tickers_map = await load_strategy_tickers()

        except Exception as e:
            print(f"[redis] Ошибка обработки сообщения: {e}", flush=True)

# --- Точка входа ---
async def main():
    global active_strategies, strategy_tickers_map
    print("[strategies] Запуск координатора...", flush=True)
    active_strategies = await load_strategies()
    strategy_tickers_map = await load_strategy_tickers()
    print(f"[init] Стратегий: {len(active_strategies)}", flush=True)
    
    asyncio.create_task(periodic_refresh())
    await redis_listener()

if __name__ == "__main__":
    asyncio.run(main())
