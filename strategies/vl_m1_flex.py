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

    async def get_price(self, symbol: str) -> Decimal:
        val = await REDIS.get(f"price:{symbol}")
        return Decimal(val.decode()) if val else None

    async def get_db(self):
        return await asyncpg.connect(DATABASE_URL)

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
                continue
            mark = Decimal(price_raw.decode()).quantize(Decimal(f"1e-{pp}"), rounding=ROUND_DOWN)

            targets = await conn.fetch("""
                SELECT id, type, level, price, quantity
                FROM position_targets
                WHERE position_id = $1 AND hit = false AND canceled = false
                ORDER BY type, level NULLS LAST
            """, pid)

            for target in targets:
                target_id = target["id"]
                t_type = target["type"]
                level = target["level"]
                t_price = target["price"]
                t_qty = target["quantity"]

                if t_type == "tp":
                    triggered = mark >= t_price if direction == "long" else mark <= t_price
                elif t_type == "sl":
                    triggered = mark <= t_price if direction == "long" else mark >= t_price
                else:
                    continue

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

                # --- SL закрывает полностью ---
                if t_type == "sl":
                    await conn.execute("""
                        UPDATE positions
                        SET status = 'closed', closed_at = now(), exit_price = $1, close_reason = 'sl'
                        WHERE id = $2
                    """, mark, pid)
                    print(f"[VL_M1_FLEX] ❌ Позиция закрыта по SL: цена={mark}", flush=True)

                # --- TP1 → перестановка SL на entry_price ---
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
                    print(f"[VL_M1_FLEX] 🔄 SL перемещён на безубыток: {entry}", flush=True)

                # --- TP2 → перестановка SL на entry + 1 ATR ---
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
                        print(f"[VL_M1_FLEX] 🔄 SL перемещён после TP2: {new_sl}", flush=True)

            # --- Финальное закрытие если всё закрыто ---
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

# --- Локальный запуск ---
if __name__ == '__main__':
    strategy = VlM1FlexStrategy(strategy_id=999)
    asyncio.run(strategy.tick())