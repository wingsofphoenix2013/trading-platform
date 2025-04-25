# strategies_main.py — координатор стратегий

import asyncio
import asyncpg
import redis.asyncio as redis
import os
import json
from datetime import datetime

# --- Блок импорта стратегий ---
import vilarso_m5_flex
import lx_m5_strict
from vl_m1_flex import VlM1FlexStrategy

# --- Конфигурация окружения ---
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# --- Глобальные переменные ---
running_strategies = {}  # имя стратегии -> объект стратегии
strategy_tickers_map = {}  # strategy_id -> set of ticker_ids

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

# --- Старт всех активных стратегий ---
async def start_active_strategies():
    global strategy_tickers_map

    print("[strategies_main] 🚀 Загрузка стратегий...")
    strategy_infos = await load_strategies()
    strategy_tickers_map = await load_strategy_tickers()

    for strategy_info in strategy_infos:
        strategy_id = strategy_info["id"]
        strategy_name = strategy_info["name"]

        # Здесь создаём объекты стратегий
        if strategy_name == "VL_M1_FLEX":
            strategy = VlM1FlexStrategy(strategy_id=strategy_id)
            asyncio.create_task(strategy.main_loop())
            running_strategies[strategy_name] = strategy
            print(f"[strategies_main] ✅ Стратегия {strategy_name} запущена")
        
        # сюда можно дописать другие стратегии в будущем
        # elif strategy_name == "VILARSO_M5_FLEX":
        #     strategy = VilarsoM5FlexStrategy(...)
        #     asyncio.create_task(strategy.main_loop())
        #     running_strategies[strategy_name] = strategy

# --- Обработка входящего сигнала ---
async def handle_signal(signal_log_id: int):
    try:
        print(f"[strategies_main] 📡 Обработка signal_log_id={signal_log_id}", flush=True)
        conn = await get_db()
        row = await conn.fetchrow("""
            SELECT s.name
            FROM signal_log_entries sle
            JOIN strategies s ON s.id = sle.strategy_id
            WHERE sle.log_id = $1
            LIMIT 1
        """, signal_log_id)
        await conn.close()

        if not row:
            print(f"[strategies_main] ⚠️ Стратегия для signal_log_id={signal_log_id} не найдена", flush=True)
            return

        strategy_name = row["name"]

        if strategy_name in running_strategies:
            strategy = running_strategies[strategy_name]
            await strategy.on_signal(signal_log_id)
        else:
            print(f"[strategies_main] ⚠️ Стратегия {strategy_name} не активна", flush=True)

    except Exception as e:
        print(f"[strategies_main] ❌ Ошибка обработки сигнала: {e}", flush=True)
        
# --- Основная точка входа ---
async def main():
    print("[strategies_main] 📋 Запуск системы стратегий", flush=True)

    await start_active_strategies()

    print("[strategies_main] ✅ Все стратегии активированы", flush=True)

    # Подключение к Redis и подписка на канал
    redis_conn = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        ssl=True
    )

    pubsub = redis_conn.pubsub()
    await pubsub.subscribe("signal_logs_ready")

    print("[strategies_main] 📡 Ожидание сигналов...", flush=True)

    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        raw_data = message['data']

        # 💡 Декодируем байты в строку
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')

        try:
            payload = json.loads(raw_data)

            # Универсальная обработка формата
            if isinstance(payload, dict):
                signal_log_id = payload.get("signal_log_id")
            elif isinstance(payload, int):
                signal_log_id = payload
            else:
                print(f"[strategies_main] ⚠️ Неподдерживаемый формат сигнала: {payload}")
                continue

            if signal_log_id:
                await handle_signal(signal_log_id)

        except Exception as e:
            print(f"[strategies_main] ❌ Ошибка обработки сигнала: {e}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())