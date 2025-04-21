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

    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT sl.ticker_symbol AS symbol, sl.direction, sle.strategy_id
            FROM signal_log_entries sle
            JOIN signal_logs sl ON sle.log_id = sl.id
            WHERE sle.log_id = $1
        """, log_id)
        await conn.close()

        if not row:
            print(f"[STRATEGY] log_id={log_id}: сигнал не найден", flush=True)
            return

        symbol = row["symbol"]
        direction = row["direction"]
        strategy_id = row["strategy_id"]

        # --- Проверка разрешения на торговлю по тикеру ---
        precision_info = await get_precision_and_permission(symbol)
        if not precision_info or precision_info["tradepermission"] != "enabled":
            print(f"[CHECK] Торговля по тикеру {symbol} отключена", flush=True)
            await update_signal_log(log_id, "ignored_by_check", f"{symbol}: trade disabled")
            return

        # --- Проверка параметров стратегии ---
        strategy_params = await get_strategy_params(strategy_id)
        if not strategy_params:
            await update_signal_log(log_id, "error", "strategy not found")
            return

        # --- Проверка текущего объёма открытых позиций ---
        conn = await get_pg_connection()
        total_open = await conn.fetchval("""
            SELECT COALESCE(SUM(notional_value), 0)
            FROM positions
            WHERE strategy_id = $1 AND status IN ('open', 'partial')
        """, strategy_id)
        await conn.close()

        if total_open + strategy_params["position_limit"] > strategy_params["deposit"]:
            print(f"[CHECK] Превышен лимит депозита: {total_open} + {strategy_params['position_limit']} > {strategy_params['deposit']}", flush=True)
            await update_signal_log(log_id, "ignored_by_check", "deposit limit exceeded")
            return

        # --- Проверка: есть ли уже открытая позиция по тикеру ---
        existing_position = await get_open_position(strategy_id, symbol)
        if existing_position:
            if existing_position["direction"] == direction:
                print(f"[CHECK] Уже открыта позиция в том же направлении — игнорируется", flush=True)
                await update_signal_log(log_id, "ignored_by_check", "already in position")
                return
            else:
                print(f"[CHECK] Обнаружен реверс — будет выполнено закрытие текущей позиции", flush=True)
                # Здесь будет логика реверса — Шаг 4
                await update_signal_log(log_id, "ignored_by_check", "reverse not implemented yet")
                return

        # Если дошли сюда — можно открывать новую позицию (Шаг 3)
        print(f"[CHECK] Все проверки пройдены — можно открывать позицию", flush=True)

    except Exception as e:
        print(f"[STRATEGY] Ошибка в process_signal: {e}", flush=True)
        await update_signal_log(log_id, "error", str(e))
