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
        # TODO: добавить фильтрацию и логику входа

    async def tick(self):
        # TODO: сюда можно добавить мониторинг TP/SL позже
        pass


# --- Локальный запуск ---
if __name__ == '__main__':
    strategy = VlM1FlexStrategy(strategy_id=999)
    asyncio.run(strategy.tick())
