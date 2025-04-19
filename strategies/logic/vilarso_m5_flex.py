# strategies/logic/vilarso_m5_flex.py

"""
Стратегия vilarso_m5_flex: реверсивная логика входов по управляющему сигналу.
Работает в обе стороны (long/short). При каждом сигнале открывает позицию в его направлении.
Если позиция уже есть и направление противоположно — переворачивается.
Если включён стоп-лосс — рассчитывает его и фиксирует в журнале.
"""

import asyncpg
from datetime import datetime
from db import get_pg_connection
from utils import get_current_price, get_latest_atr, update_signal_log

# Основная точка входа
async def process_signal(log_id: int):
    print(f"[STRATEGY] vilarso_m5_flex: запуск обработки log_id={log_id}", flush=True)

    try:
        pg: asyncpg.Connection = await get_pg_connection()

        # --- Загрузка строки из журнала сигналов
        row = await pg.fetchrow("""
            SELECT sl.log_id, sl.symbol, sl.signal_direction, sl.strategy_id,
                   s.name AS strategy_name, s.enabled, s.deposit, s.position_limit,
                   s.use_all_tickers, s.use_stoploss, s.sl_type, s.sl_value,
                   sig.name AS signal_name, sig.id AS signal_id, s.action_signal_id
            FROM signal_log_entries sl
            JOIN strategies s ON s.id = sl.strategy_id
            JOIN signals sig ON sig.id = sl.signal_id
            WHERE sl.log_id = $1
        """, log_id)

        if not row:
            print(f"[STRATEGY] log_id={log_id}: не найден в базе", flush=True)
            return

        if not row["enabled"]:
            print(f"[STRATEGY] стратегия отключена (id={row['strategy_id']})", flush=True)
            await update_signal_log(log_id, "ignored", "strategy disabled")
            return

        # --- Проверка: управляющий ли это сигнал
        if row["signal_id"] != row["action_signal_id"]:
            print(f"[STRATEGY] сигнал не является управляющим, пропуск", flush=True)
            await update_signal_log(log_id, "ignored", "not action signal")
            return

        symbol = row["symbol"]
        signal_dir = row["signal_direction"]  # 'long' или 'short'
        use_sl = row["use_stoploss"]
        sl_type = row["sl_type"]
        sl_value = row["sl_value"]

        # --- Получение текущей цены
        entry_price = await get_current_price(symbol)
        if entry_price is None:
            print(f"[STRATEGY] не удалось получить цену для {symbol}", flush=True)
            await update_signal_log(log_id, "error", "no price")
            return

        sl_note = "SL: none"

        # --- Расчёт SL, если включён
        if use_sl:
            if sl_type == "percent":
                if signal_dir == "long":
                    sl_price = entry_price * (1 - sl_value / 100)
                else:
                    sl_price = entry_price * (1 + sl_value / 100)
                sl_note = f"SL ({sl_type}) = {sl_price:.6f}"

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
                sl_note = f"SL ({sl_type}) = {sl_price:.6f} (ATR={atr:.6f})"

        # --- Заглушка: считаем, что позиции нет
        note = f"open {signal_dir} @ {entry_price:.6f}; {sl_note}"
        await update_signal_log(log_id, "approved", note)
        print(f"[STRATEGY] log_id={log_id}: {note}", flush=True)

    except Exception as e:
        print(f"[STRATEGY] Ошибка при обработке log_id={log_id}: {e}", flush=True)
        await update_signal_log(log_id, "error", str(e))