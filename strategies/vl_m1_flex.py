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

class VlM1FlexStrategy:
    def __init__(self, strategy_id: int):
        self.strategy_id = strategy_id

    async def get_price(self, symbol: str) -> Decimal:
        val = await REDIS.get(f"price:{symbol}")
        return Decimal(val) if val else None

    async def get_ema(self, symbol: str, tf: str = "M1") -> Decimal:
        val = await REDIS.get(f"{symbol}:{tf}:EMA:50")
        return Decimal(val) if val else None

    async def get_atr(self, symbol: str, tf: str = "M1") -> Decimal:
        val = await REDIS.get(f"{symbol}:{tf}:ATR:atr")
        return Decimal(val) if val else None

    async def get_db(self):
        return await asyncpg.connect(DATABASE_URL)

    async def on_signal(self, log_id: int):
        print(f"[VL_M1_FLEX] Обработка сигнала log_id={log_id}", flush=True)
        conn = await self.get_db()

        # --- Получение базовой информации ---
        row = await conn.fetchrow("""
            SELECT sl.ticker_symbol, sl.direction, s.deposit, s.position_limit, s.use_all_tickers
            FROM signal_logs sl
            JOIN strategies s ON s.id = $1
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

        if ok:
            print(f"[VL_M1_FLEX] ✅ {direction.upper()} разрешён: цена={price}, EMA={ema}, ATR={atr}, порог={required_price}", flush=True)
        else:
            print(f"[VL_M1_FLEX] ❌ {direction.upper()} запрещён: цена={price}, EMA={ema}, ATR={atr}, порог={required_price}", flush=True)

        await conn.close()

    async def tick(self):
        pass


# --- Локальный запуск ---
if __name__ == '__main__':
    strategy = VlM1FlexStrategy(strategy_id=999)
    asyncio.run(strategy.tick())
