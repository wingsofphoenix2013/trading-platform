import asyncpg
import logging
import redis.asyncio as redis
import os
from decimal import Decimal, ROUND_DOWN

class StrategyInterface:
    def __init__(self, database_url, open_positions=None):
        self.database_url = database_url
        self.open_positions = open_positions

    # 🔹 Подключение к Redis (для цен, индикаторов)
    def get_redis(self):
        return redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            ssl=True
        )

    # 🔹 Получение параметров стратегии
    async def get_strategy_params(self, strategy_name):
        conn = await asyncpg.connect(self.database_url)
        try:
            query = """
                SELECT * FROM strategies_v2
                WHERE name = $1 AND enabled = true AND archived = false
            """
            return await conn.fetchrow(query, strategy_name)
        finally:
            await conn.close()

    # 🔹 Логирование действий стратегии
    async def log_strategy_action(self, log_id, strategy_id, status, position_id=None, note=None):
        conn = await asyncpg.connect(self.database_url)
        try:
            query = """
                INSERT INTO signal_log_entries
                    (log_id, strategy_id, status, position_id, note, logged_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """
            await conn.execute(query, log_id, strategy_id, status, position_id, note)
        except Exception as e:
            logging.error(f"Ошибка логирования действия стратегии: {e}")
        finally:
            await conn.close()

    # 🔹 Получение текущей цены из Redis
    async def get_current_price(self, symbol):
        redis_client = self.get_redis()
        price_str = await redis_client.get(f"price:{symbol}")
        await redis_client.close()
        if not price_str:
            logging.warning(f"Нет текущей цены для {symbol}")
            return None
        try:
            return Decimal(price_str)
        except Exception as e:
            logging.error(f"Ошибка преобразования цены {symbol}: {e}")
            return None