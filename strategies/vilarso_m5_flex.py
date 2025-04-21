### Стратегия vilarso_m5_flex — реверсивная логика входа и выхода

"""
Стратегия vilarso_m5_flex: реверсивная логика входов по управляющему сигналу.
Работает в обе стороны (long/short). При каждом сигнале открывает позицию в его направлении.
Если включён стоп-лосс — рассчитывает его и фиксирует в журнале.
"""

import os
import asyncpg
from datetime import datetime

# --- Подключение к БД ---
async def get_pg_connection():
    db_url = os.getenv("DATABASE_URL")
    return await asyncpg.connect(db_url)

# --- Получение текущей цены из Redis (ключ price:{symbol}) ---
async def get_current_price(symbol: str):
    try:
        import redis.asyncio as redis_lib
        redis = redis_lib.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD"),
            ssl=True
        )
        price_str = await redis.get(f"price:{symbol}")
        return float(price_str) if price_str else None
    except Exception as e:
        print(f"[get_current_price] Redis error for {symbol}: {e}", flush=True)
        return None

# --- Получение последнего значения ATR ---
async def get_latest_atr(symbol: str):
    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT atr
            FROM ohlcv_m5
            WHERE symbol = $1 AND atr IS NOT NULL
            ORDER BY open_time DESC
            LIMIT 1
        """, symbol)
        await conn.close()
        return float(row["atr"]) if row else None
    except Exception as e:
        print(f"[get_latest_atr] Ошибка для {symbol}: {e}", flush=True)
        return None

# --- Получение точности округления и разрешения на торговлю по тикеру ---
async def get_precision_and_permission(symbol: str):
    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT precision_price, precision_qty, tradepermission
            FROM tickers
            WHERE symbol = $1
        """, symbol)
        await conn.close()
        if not row:
            print(f"[get_precision_and_permission] Тикер {symbol} не найден", flush=True)
            return None
        return {
            "precision_price": int(row["precision_price"]),
            "precision_qty": int(row["precision_qty"]),
            "tradepermission": row["tradepermission"]
        }
    except Exception as e:
        print(f"[get_precision_and_permission] Ошибка при обработке {symbol}: {e}", flush=True)
        return None

# --- Получение параметров стратегии ---
async def get_strategy_params(strategy_id: int):
    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT deposit, position_limit, use_stoploss, sl_type, sl_value
            FROM strategies
            WHERE id = $1
        """, strategy_id)
        await conn.close()
        if not row:
            print(f"[get_strategy_params] Стратегия {strategy_id} не найдена", flush=True)
            return None
        return {
            "deposit": float(row["deposit"]),
            "position_limit": float(row["position_limit"]),
            "use_stoploss": row["use_stoploss"],
            "sl_type": row["sl_type"],
            "sl_value": float(row["sl_value"])
        }
    except Exception as e:
        print(f"[get_strategy_params] Ошибка при обработке strategy_id={strategy_id}: {e}", flush=True)
        return None

# --- Получение открытой позиции по стратегии и тикеру ---
async def get_open_position(strategy_id: int, symbol: str):
    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT *
            FROM positions
            WHERE strategy_id = $1 AND symbol = $2 AND status IN ('open', 'partial')
            ORDER BY created_at DESC
            LIMIT 1
        """, strategy_id, symbol)
        await conn.close()
        return row
    except Exception as e:
        print(f"[get_open_position] Ошибка при проверке позиции по {symbol}: {e}", flush=True)
        return None

# --- Обновление записи в журнале действий стратегии ---
async def update_signal_log(log_id: int, status: str, note: str):
    try:
        conn = await get_pg_connection()
        await conn.execute("""
            UPDATE signal_log_entries
            SET status = $1,
                note = $2,
                logged_at = NOW()
            WHERE log_id = $3
        """, status, note, log_id)
        await conn.close()
    except Exception as e:
        print(f"[update_signal_log] Ошибка при обновлении log_id={log_id}: {e}", flush=True)

# --- Основная функция обработки сигнала ---
async def process_signal(log_id: int):
    print(f"[STRATEGY] vilarso_m5_flex: запуск обработки log_id={log_id}", flush=True)
    # Реализация логики будет добавлена на следующих этапах
    pass
