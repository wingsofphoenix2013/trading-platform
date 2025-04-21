### Стратегия vilarso_m5_flex — реверсивная логика входа и выхода

"""
Стратегия vilarso_m5_flex: реверсивная логика входов по управляющему сигналу.
Работает в обе стороны (long/short). При каждом сигнале открывает позицию в его направлении.
Если включён стоп-лосс — рассчитывает его и фиксирует в журнале.
"""

import os
import asyncpg
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

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

        precision_info = await get_precision_and_permission(symbol)
        if not precision_info or precision_info["tradepermission"] != "enabled":
            print(f"[CHECK] Торговля по тикеру {symbol} отключена", flush=True)
            await update_signal_log(log_id, "ignored_by_check", f"{symbol}: trade disabled")
            return

        strategy_params = await get_strategy_params(strategy_id)
        if not strategy_params:
            await update_signal_log(log_id, "error", "strategy not found")
            return

        conn = await get_pg_connection()
        total_open = float(await conn.fetchval("""
            SELECT COALESCE(SUM(notional_value), 0)
            FROM positions
            WHERE strategy_id = $1 AND status IN ('open', 'partial')
        """, strategy_id))
        await conn.close()

        if total_open + strategy_params["position_limit"] > strategy_params["deposit"]:
            print(f"[CHECK] Превышен лимит депозита: {total_open} + {strategy_params['position_limit']} > {strategy_params['deposit']}", flush=True)
            await update_signal_log(log_id, "ignored_by_check", "deposit limit exceeded")
            return

        existing_position = await get_open_position(strategy_id, symbol)
        if existing_position:
            if existing_position["direction"] == direction:
                print(f"[CHECK] Уже открыта позиция в том же направлении — игнорируется", flush=True)
                await update_signal_log(log_id, "ignored_by_check", "already in position")
                return
            else:
                print(f"[CHECK] Обнаружен реверс — будет выполнено закрытие текущей позиции", flush=True)
                await update_signal_log(log_id, "ignored_by_check", "reverse not implemented yet")
                return

        # --- Открытие новой позиции с точным округлением через Decimal ---
        entry_price_raw = await get_current_price(symbol)
        if entry_price_raw is None:
            await update_signal_log(log_id, "error", "price not available")
            return

        precision_price = precision_info["precision_price"]
        precision_qty = precision_info["precision_qty"]

        entry_price = Decimal(entry_price_raw).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
        position_limit = Decimal(strategy_params["position_limit"])
        quantity = (position_limit / entry_price).quantize(Decimal(f'1e-{precision_qty}'), rounding=ROUND_DOWN)
        notional_value = (entry_price * quantity).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

        conn = await get_pg_connection()
        result = await conn.fetchrow("""
            INSERT INTO positions (
                strategy_id, log_id, symbol, direction,
                entry_price, quantity, quantity_left,
                notional_value, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $6, $7, 'open', NOW())
            RETURNING id
        """, strategy_id, log_id, symbol, direction, entry_price, quantity, notional_value)
        position_id = result["id"]

        if strategy_params["use_stoploss"]:
            if strategy_params["sl_type"] == "percent":
                sl_offset = (entry_price * Decimal(strategy_params["sl_value"] / 100)).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
            elif strategy_params["sl_type"] == "atr":
                atr = await get_latest_atr(symbol)
                if atr is None:
                    await update_signal_log(log_id, "error", "ATR not available")
                    await conn.close()
                    return
                sl_offset = (Decimal(atr) * Decimal(strategy_params["sl_value"])).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
            else:
                sl_offset = Decimal("0")

            sl_price = entry_price - sl_offset if direction == "long" else entry_price + sl_offset
            sl_price = sl_price.quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

            await conn.execute("""
                INSERT INTO position_targets (position_id, type, price, quantity, hit)
                VALUES ($1, 'sl', $2, $3, false)
            """, position_id, sl_price, quantity)

        tp_offset = (entry_price * Decimal("0.005")).quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
        tp_price = entry_price + tp_offset if direction == "long" else entry_price - tp_offset
        tp_price = tp_price.quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)
        tp_quantity = (quantity * Decimal("0.5")).quantize(Decimal(f'1e-{precision_qty}'), rounding=ROUND_DOWN)

        await conn.execute("""
            INSERT INTO position_targets (position_id, type, level, price, quantity, hit)
            VALUES ($1, 'tp', 1, $2, $3, false)
        """, position_id, tp_price, tp_quantity)

        await update_signal_log(log_id, "position_opened", f"entry={str(entry_price)}, qty={str(quantity)}")
        await conn.close()

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[STRATEGY] Ошибка в process_signal: {e}", flush=True)
        await update_signal_log(log_id, "error", str(e))
