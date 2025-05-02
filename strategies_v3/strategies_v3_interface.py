# 🔸 Импорты и базовая настройка
import os
import asyncpg
import redis.asyncio as aioredis
from decimal import Decimal, ROUND_DOWN

# 🔸 Интерфейс стратегий v3
class StrategyInterface:
    def __init__(self):
        self.pg_dsn = os.environ["DATABASE_URL"]
        self.redis_url = os.environ["REDIS_URL"]
        self._pg_pool = None
        self._redis = None

    # 🔸 Получение подключения к Redis (Upstash)
    async def get_redis(self):
        if not self._redis:
            self._redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    # 🔸 Получение пула PostgreSQL
    async def get_pg(self):
        if not self._pg_pool:
            self._pg_pool = await asyncpg.create_pool(dsn=self.pg_dsn)
        return self._pg_pool

    # 🔸 Загрузка тикеров с precision
    async def load_tickers(self):
        pg = await self.get_pg()
        query = """
        SELECT symbol, precision_price, precision_qty, min_qty, tradepermission
        FROM tickers
        WHERE status = 'enabled'
        """
        rows = await pg.fetch(query)
        return {
            row["symbol"]: {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"],
                "min_qty": row["min_qty"],
                "tradepermission": row["tradepermission"],
            }
            for row in rows
        }

    # 🔸 Получение параметров стратегии
    async def get_strategy_params(self, strategy_name: str):
        pg = await self.get_pg()
        row = await pg.fetchrow("""
            SELECT * FROM strategies_v2 WHERE name = $1 AND enabled = true AND archived = false
        """, strategy_name)
        return dict(row) if row else None

    # 🔸 Логирование действия стратегии
    async def log_strategy_action(self, *, log_id: int, strategy_id: int, status: str, position_id: int = None, note: str = None):
        pg = await self.get_pg()
        await pg.execute("""
            INSERT INTO signal_log_entries_v2 (log_id, strategy_id, status, position_id, note)
            VALUES ($1, $2, $3, $4, $5)
        """, log_id, strategy_id, status, position_id, note)