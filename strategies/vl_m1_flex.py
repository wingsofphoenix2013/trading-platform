# VL_M1_FLEX — автономная стратегия как класс

import asyncio
import asyncpg
import redis.asyncio as redis
import os
from decimal import Decimal, ROUND_DOWN
from datetime import datetime

# --- Конфигурация ---
REDIS = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=True
)

DATABASE_URL = os.getenv("DATABASE_URL")
COMMISSION_RATE = Decimal("0.0004")  # 0.04% комиссия биржи

class VlM1FlexStrategy:
    def __init__(self, strategy_id: int):
        self.strategy_id = strategy_id
        self.last_sl_shift = None  # отслеживание последней перестановки SL

    async def get_price(self, symbol: str) -> Decimal:
        val = await REDIS.get(f"price:{symbol}")
        return Decimal(val.decode()) if val else None

    async def get_ema(self, symbol: str, tf: str = "M1") -> Decimal:
        val = await REDIS.get(f"{symbol}:{tf}:EMA:50")
        return Decimal(val.decode()) if val else None

    async def get_atr(self, symbol: str, tf: str = "M1") -> Decimal:
        val = await REDIS.get(f"{symbol}:{tf}:ATR:atr")
        return Decimal(val.decode()) if val else None

    async def get_db(self):
        return await asyncpg.connect(DATABASE_URL)

    async def on_signal(self, log_id: int):
        print(f"[VL_M1_FLEX] Обработка сигнала log_id={log_id}", flush=True)
        conn = await self.get_db()

        # --- Получение базовой информации ---
        row = await conn.fetchrow("""
            SELECT sl.ticker_symbol, sl.direction, s.deposit, s.position_limit, s.use_all_tickers,
                   t.precision_price, t.precision_qty, t.min_qty
            FROM signal_logs sl
            JOIN strategies s ON s.id = $1
            JOIN tickers t ON t.symbol = sl.ticker_symbol
            WHERE sl.id = $2
        """, self.strategy_id, log_id)

        if not row:
            print(f"[VL_M1_FLEX] log_id={log_id} не найден", flush=True)
            return

        symbol = row["ticker_symbol"]
        direction = row["direction"]
        deposit = row["deposit"]
        limit = row["position_limit"]
        use_all = row["use_all_tickers"]
        pp = row["precision_price"]
        pq = row["precision_qty"]
        min_qty = row["min_qty"]

        # --- Проверка: разрешён ли тикер ---
        if not use_all:
            res = await conn.fetchval("""
                SELECT COUNT(*) FROM strategy_tickers st
                JOIN tickers t ON st.ticker_id = t.id
                WHERE st.strategy_id = $1 AND t.symbol = $2 AND st.enabled = true
            """, self.strategy_id, symbol)
            if res == 0:
                print(f"[VL_M1_FLEX] ❌ {symbol} запрещён для стратегии", flush=True)
                await conn.close()
                return
                
        # --- Проверка: есть ли уже открытая сделка по тикеру ---
        active_pos = await conn.fetchval("""
            SELECT COUNT(*)
            FROM positions
            WHERE strategy_id = $1 AND symbol = $2 AND status = 'open'
        """, self.strategy_id, symbol)

        if active_pos > 0:
            print(f"[VL_M1_FLEX] ❌ Уже есть открытая позиция по {symbol}, сигнал игнорируется", flush=True)
            await conn.close()
            return                

        # --- Проверка: превышен ли депозит ---
        total_notional = await conn.fetchval("""
            SELECT COALESCE(SUM(notional_value), 0)
            FROM positions
            WHERE strategy_id = $1 AND status = 'open'
        """, self.strategy_id)

        if total_notional >= deposit:
            print(f"[VL_M1_FLEX] ❌ Депозит исчерпан: {total_notional} / {deposit}", flush=True)
            await conn.close()
            return

        # --- Проверка EMA + 0.5 ATR ---
        price = await self.get_price(symbol)
        ema = await self.get_ema(symbol)
        atr = await self.get_atr(symbol)

        if not all([price, ema, atr]):
            print(f"[VL_M1_FLEX] Недостаточно данных: price={price}, ema={ema}, atr={atr}", flush=True)
            await conn.close()
            return

        if direction == "long":
            required_price = ema - (Decimal("0.5") * atr)
            ok = price >= required_price
        elif direction == "short":
            required_price = ema + (Decimal("0.5") * atr)
            ok = price <= required_price
        else:
            print(f"[VL_M1_FLEX] Неизвестное направление: {direction}", flush=True)
            await conn.close()
            return

        if not ok:
            print(f"[VL_M1_FLEX] ❌ {direction.upper()} запрещён: цена={price}, EMA={ema}, ATR={atr}, порог={required_price}", flush=True)
            await conn.close()
            return

        print(f"[VL_M1_FLEX] ✅ {direction.upper()} разрешён: цена={price}, EMA={ema}, ATR={atr}, порог={required_price}", flush=True)

        # --- Расчёт объёма позиции ---
        notional_target = limit
        qty = (notional_target / price).quantize(Decimal(f"1e-{pq}"), rounding=ROUND_DOWN)

        if qty < min_qty:
            print(f"[VL_M1_FLEX] ❌ qty={qty} меньше min_qty={min_qty}", flush=True)
            await conn.close()
            return

        notional_final = (qty * price).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
        print(f"[VL_M1_FLEX] 💰 Объём позиции: qty={qty}, notional={notional_final}", flush=True)

        # --- Создание позиции ---
        commission = (notional_final * COMMISSION_RATE).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
        pnl = -commission

        position_id = await conn.fetchval("""
            INSERT INTO positions (strategy_id, log_id, symbol, direction, entry_price, quantity,
                quantity_left, notional_value, status, created_at, pnl)
            VALUES ($1, $2, $3, $4, $5, $6, $6, $7, 'open', now(), $8)
            RETURNING id
        """, self.strategy_id, log_id, symbol, direction, price, qty, notional_final, pnl)

        print(f"[VL_M1_FLEX] ✅ Позиция открыта id={position_id}, комиссия={commission}", flush=True)

        # --- Создание целей ---
        targets = [
            ("tp", 1, price + (Decimal("1.5") * atr), qty * Decimal("0.5")),
            ("tp", 2, price + (Decimal("2.5") * atr), qty * Decimal("0.3")),
            ("tp", 3, price + (Decimal("3.5") * atr), qty * Decimal("0.2")),
            ("sl", None, price - (Decimal("1.5") * atr), qty)
        ] if direction == "long" else [
            ("tp", 1, price - (Decimal("1.5") * atr), qty * Decimal("0.5")),
            ("tp", 2, price - (Decimal("2.5") * atr), qty * Decimal("0.3")),
            ("tp", 3, price - (Decimal("3.5") * atr), qty * Decimal("0.2")),
            ("sl", None, price + (Decimal("1.5") * atr), qty)
        ]

        for t_type, level, t_price, t_qty in targets:
            t_price = t_price.quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
            t_qty = t_qty.quantize(Decimal(f"1e-{pq}"), rounding=ROUND_DOWN)
            await conn.execute("""
                INSERT INTO position_targets (position_id, type, level, price, quantity)
                VALUES ($1, $2, $3, $4, $5)
            """, position_id, t_type, level, t_price, t_qty)

        await conn.execute("""
            INSERT INTO signal_log_entries (strategy_id, log_id, status, position_id, note, logged_at)
            VALUES ($1, $2, 'position_opened', $3, 'position created', now())
        """, self.strategy_id, log_id, position_id)

        await conn.close()

    async def tick(self):
        print("[VL_M1_FLEX] ⏱ Проверка активных позиций...", flush=True)
        conn = await self.get_db()

        rows = await conn.fetch("""
            SELECT p.id, p.symbol, p.direction, p.entry_price, p.quantity_left, p.quantity, p.pnl,
                   t.precision_price, t.precision_qty
            FROM positions p
            JOIN tickers t ON t.symbol = p.symbol
            WHERE p.strategy_id = $1 AND p.status = 'open'
        """, self.strategy_id)

        print(f"[DEBUG] Найдено открытых позиций: {len(rows)}", flush=True)

        for row in rows:
            pid = row["id"]
            symbol = row["symbol"]
            direction = row["direction"]
            entry = Decimal(row["entry_price"])
            qty_left = Decimal(row["quantity_left"])
            qty_total = Decimal(row["quantity"])
            pnl = Decimal(row["pnl"])
            pp = row["precision_price"]
            pq = row["precision_qty"]

            price_raw = await REDIS.get(f"price:{symbol}")
            if not price_raw:
                print(f"[DEBUG] Цена не найдена в Redis для {symbol}", flush=True)
                continue

            mark = Decimal(price_raw.decode()).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
            print(f"[DEBUG] Цена {symbol} сейчас: {mark}", flush=True)

            targets = await conn.fetch("""
                SELECT id, type, level, price, quantity
                FROM position_targets
                WHERE position_id = $1 AND hit = false AND canceled = false
                ORDER BY type, level NULLS LAST
            """, pid)

            print(f"[DEBUG] Целей для позиции {pid}: {len(targets)}", flush=True)

            for target in targets:
                target_id = target["id"]
                t_type = target["type"]
                level = target["level"]
                t_price = target["price"]
                t_qty = target["quantity"]

                triggered = False
                if t_type == "tp":
                    triggered = mark >= t_price if direction == "long" else mark <= t_price
                elif t_type == "sl":
                    triggered = mark <= t_price if direction == "long" else mark >= t_price
                else:
                    continue

                print(f"[DEBUG] Проверка цели {t_type.upper()} для {symbol}: текущая цена={mark}, цель={t_price}, triggered={triggered}", flush=True)

                if not triggered:
                    continue

                print(f"[VL_M1_FLEX] 🎯 Цель сработала: {t_type.upper()} L{level or '-'} @ {t_price} qty={t_qty}", flush=True)

                commission = (mark * t_qty * COMMISSION_RATE).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
                delta = (mark - entry if direction == "long" else entry - mark) * t_qty
                pnl_step = delta - commission

                await conn.execute("""
                    UPDATE position_targets
                    SET hit = true, hit_at = now()
                    WHERE id = $1
                """, target_id)

                await conn.execute("""
                    UPDATE positions
                    SET quantity_left = quantity_left - $1,
                        pnl = pnl + $2
                    WHERE id = $3
                """, t_qty, pnl_step, pid)

                await conn.execute("""
                    INSERT INTO signal_log_entries (strategy_id, log_id, status, position_id, note, logged_at)
                    VALUES ($1, NULL, $2, $3, $4, now())
                """, self.strategy_id, f"{t_type}_hit", pid, f"{t_type} level {level or '-'} hit")

                if t_type == "tp" and level in (1, 2):
                    await conn.execute("""
                        UPDATE positions
                        SET close_reason = $1
                        WHERE id = $2
                    """, f"tp{level}-hit", pid)

                if t_type == "sl":
                    reason = 'sl'
                    if self.last_sl_shift == "tp1":
                        reason = 'sl-after-tp1'
                    elif self.last_sl_shift == "tp2":
                        reason = 'sl-after-tp2'

                    await conn.execute("""
                        UPDATE positions
                        SET status = 'closed', closed_at = now(), exit_price = $1, close_reason = $2
                        WHERE id = $3
                    """, mark, reason, pid)

                    print(f"[VL_M1_FLEX] ❌ Позиция закрыта по {reason.upper()}: цена={mark}", flush=True)
                    self.last_sl_shift = None

                elif t_type == "tp" and level == 1:
                    await conn.execute("""
                        UPDATE position_targets
                        SET canceled = true
                        WHERE position_id = $1 AND type = 'sl' AND hit = false AND canceled = false
                    """, pid)
                    await conn.execute("""
                        INSERT INTO position_targets (position_id, type, level, price, quantity)
                        VALUES ($1, 'sl', NULL, $2, $3)
                    """, pid, entry, qty_left - t_qty)
                    self.last_sl_shift = "tp1"
                    print(f"[VL_M1_FLEX] 🔄 SL перемещён на безубыток: {entry}", flush=True)

                elif t_type == "tp" and level == 2:
                    atr_val = await REDIS.get(f"{symbol}:M1:ATR:atr")
                    if atr_val:
                        atr = Decimal(atr_val.decode())
                        new_sl = (entry + atr if direction == "long" else entry - atr).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)
                        await conn.execute("""
                            UPDATE position_targets
                            SET canceled = true
                            WHERE position_id = $1 AND type = 'sl' AND hit = false AND canceled = false
                        """, pid)
                        await conn.execute("""
                            INSERT INTO position_targets (position_id, type, level, price, quantity)
                            VALUES ($1, 'sl', NULL, $2, $3)
                        """, pid, new_sl, qty_left - t_qty)
                        self.last_sl_shift = "tp2"
                        print(f"[VL_M1_FLEX] 🔄 SL перемещён после TP2: {new_sl}", flush=True)

            final_qty = await conn.fetchval("""
                SELECT quantity_left FROM positions WHERE id = $1
            """, pid)

            if final_qty <= 0:
                await conn.execute("""
                    UPDATE positions
                    SET status = 'closed', closed_at = now(), exit_price = $1, close_reason = 'tp3'
                    WHERE id = $2
                """, mark, pid)
                print(f"[VL_M1_FLEX] ✅ Позиция полностью закрыта: id={pid}, по цене={mark}", flush=True)

        await conn.close()
        
    async def main_loop(self):
        print("[VL_M1_FLEX] 🚀 Старт основного цикла стратегии", flush=True)
        while True:
            try:
                await self.tick()
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[VL_M1_FLEX] ❌ Ошибка в основном цикле: {e}", flush=True)
                await asyncio.sleep(5)

# --- Локальный запуск ---
if __name__ == '__main__':
    strategy = VlM1FlexStrategy(strategy_id=1)
    asyncio.run(strategy.main_loop())