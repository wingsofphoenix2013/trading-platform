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

# --- Получение текущей цены из Redis (ключ price:{symbol})
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

# --- Получение последнего значения ATR
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

# --- Получение точности цены из тикера
async def get_price_precision(symbol: str):
    try:
        conn = await get_pg_connection()
        row = await conn.fetchrow("""
            SELECT precision_price
            FROM tickers
            WHERE symbol = $1
        """, symbol)
        await conn.close()
        return int(row["precision_price"]) if row else 6
    except Exception as e:
        print(f"[get_price_precision] Ошибка получения точности для {symbol}: {e}", flush=True)
        return 6

# --- Обновление записи в журнале действий стратегии
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

# --- Основная функция стратегии ---
async def process_signal(log_id: int):
    print(f"[STRATEGY] vilarso_m5_flex: запуск обработки log_id={log_id}", flush=True)

    try:
        pg: asyncpg.Connection = await get_pg_connection()

        # --- Загрузка строки из журнала сигналов и параметров стратегии
        row = await pg.fetchrow("""
            SELECT sle.id AS entry_id,
                   sg.name AS signal_name,
                   sl.ticker_symbol AS symbol,
                   sl.direction AS signal_direction,
                   sle.strategy_id,
                   s.name AS strategy_name,
                   s.enabled,
                   s.deposit,
                   s.position_limit,
                   s.use_all_tickers,
                   s.use_stoploss,
                   s.sl_type,
                   s.sl_value
            FROM signal_log_entries sle
            JOIN signal_logs sl ON sle.log_id = sl.id
            JOIN signals sg ON sl.signal_id = sg.id
            JOIN strategies s ON sle.strategy_id = s.id
            WHERE sle.log_id = $1
        """, log_id)

        await pg.close()

        if not row:
            print(f"[STRATEGY] log_id={log_id}: не найден в базе", flush=True)
            return

        if not row["enabled"]:
            print(f"[STRATEGY] стратегия отключена (id={row['strategy_id']})", flush=True)
            await update_signal_log(log_id, "ignored", "strategy disabled")
            return

        symbol = row["symbol"]
        signal_dir = row["signal_direction"]  # 'long' или 'short'
        use_sl = row["use_stoploss"]
        sl_type = row["sl_type"]
        sl_value = float(row["sl_value"])

        # --- Получение текущей цены
        entry_price = await get_current_price(symbol)
        if entry_price is None:
            print(f"[STRATEGY] не удалось получить цену для {symbol}", flush=True)
            await update_signal_log(log_id, "error", "no price")
            return

        # --- Получение точности округления
        precision = await get_price_precision(symbol)

        sl_note = "SL: none"

        # --- Расчёт SL, если включён
        if use_sl:
            if sl_type == "percent":
                if signal_dir == "long":
                    sl_price = entry_price * (1 - sl_value / 100)
                else:
                    sl_price = entry_price * (1 + sl_value / 100)
                sl_price = round(sl_price, precision)
                sl_note = f"SL (percent) = {sl_price}"

            elif sl_type == "atr":
                atr = await get_latest_atr(symbol)
                if atr is None:
                    print(f"[STRATEGY] не удалось получить ATR для {symbol}", flush=True)
                    await update_signal_log(log_id, "error", "no ATR")
                    return

                if signal_dir == "long":
                    sl_price = entry_price - sl_value * atr
                else:
                    sl_price = entry_price + sl_value * atr
                sl_price = round(sl_price, precision)
                sl_note = f"SL (ATR) = {sl_price} (ATR={round(atr, precision)})"

        # --- Пока считаем, что позиции нет — всегда открываем
        note = f"open {signal_dir} @ {entry_price:.{precision}f}; {sl_note}"
        await update_signal_log(log_id, "approved", note)
        print(f"[STRATEGY] log_id={log_id}: {note}", flush=True)

    except Exception as e:
        print(f"[STRATEGY] Ошибка при обработке log_id={log_id}: {e}", flush=True)
        await update_signal_log(log_id, "error", str(e))