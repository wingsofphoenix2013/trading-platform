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
        pass


# --- Локальный запуск ---
if __name__ == '__main__':
    strategy = VlM1FlexStrategy(strategy_id=999)
    asyncio.run(strategy.tick())
