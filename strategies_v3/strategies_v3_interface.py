# 🔸 Импорты и базовая настройка
import os
import asyncpg
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN

# 🔸 Интерфейс стратегий v3
class StrategyInterface:
    def __init__(self):
        self.pg_dsn = os.getenv("DATABASE_URL")
        self._pg_pool = None
        self._redis = None

    # 🔸 Получение подключения к Redis (Upstash / локально)
    async def get_redis(self):
        if not self._redis:
            host = os.getenv("REDIS_HOST")
            port = int(os.getenv("REDIS_PORT", "6379"))
            password = os.getenv("REDIS_PASSWORD")
            self._redis = redis.Redis(
                host=host,
                port=port,
                password=password,
                decode_responses=True,
                ssl=True
            )
        return self._redis

    # 🔸 Получение пула PostgreSQL
    async def get_pg(self):
        if not self._pg_pool:
            self._pg_pool = await asyncpg.create_pool(dsn=self.pg_dsn, min_size=1, max_size=5)
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
    # 🔸 Исключение тикера из стратегии (моментально + в БД)
    async def disable_symbol_for_strategy(self, strategy_name: str, symbol: str):
        pg = await self.get_pg()

        # 🔹 Получение ID стратегии
        strategy_row = await pg.fetchrow(
            "SELECT id FROM strategies_v2 WHERE name = $1", strategy_name
        )
        if not strategy_row:
            raise ValueError(f"Стратегия не найдена: {strategy_name}")
        strategy_id = strategy_row["id"]

        # 🔹 Получение ID тикера
        ticker_row = await pg.fetchrow(
            "SELECT id FROM tickers WHERE symbol = $1", symbol
        )
        if not ticker_row:
            raise ValueError(f"Тикер не найден: {symbol}")
        ticker_id = ticker_row["id"]

        # 🔹 Обновление strategy_tickers_v2
        await pg.execute("""
            UPDATE strategy_tickers_v2
            SET enabled = false
            WHERE strategy_id = $1 AND ticker_id = $2
        """, strategy_id, ticker_id)

        # 🔹 Моментальное исключение из памяти
        from strategies_v3_main import allowed_symbols
        allowed_symbols.get(strategy_name, set()).discard(symbol)
        
    # 🔸 Логирование действия стратегии
    async def log_strategy_action(self, *, log_id: int, strategy_id: int, status: str, position_id: int = None, note: str = None):
        pg = await self.get_pg()
        await pg.execute("""
            INSERT INTO signal_log_entries_v2 (log_id, strategy_id, status, position_id, note)
            VALUES ($1, $2, $3, $4, $5)
        """, log_id, strategy_id, status, position_id, note)