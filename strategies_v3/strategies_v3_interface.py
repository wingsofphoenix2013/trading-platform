import asyncpg
import logging
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN
import os


# 🔸 Универсальный интерфейс для стратегий v3
class StrategyInterface:
    def __init__(self, redis_client, database_url, strategies_cache, strategy_allowed_tickers, open_positions):
        # 🔸 Подключение к Redis и базе данных
        self.redis = redis_client
        self.database_url = database_url
        self.strategies_cache = strategies_cache
        self.strategy_allowed_tickers = strategy_allowed_tickers
        self.open_positions = open_positions

    # 🔸 Логирование действия стратегии в signal_log_entries_v2
    async def log_strategy_action(self, strategy_id: int, log_id: int, status: str, note: str, position_id: int = None):
        try:
            conn = await asyncpg.connect(self.database_url)
            await conn.execute("""
                INSERT INTO signal_log_entries_v2 (strategy_id, log_id, status, note, position_id, logged_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, strategy_id, log_id, status, note, position_id)
            await conn.close()

            logging.info(f"📝 Запись в лог: strategy_id={strategy_id}, log_id={log_id}, status={status}")
        except Exception as e:
            logging.error(f"❌ Ошибка при записи в signal_log_entries_v2: {e}")

    # 🔸 Базовые проверки перед открытием позиции
    async def run_basic_checks(self, task: dict) -> tuple[bool, str]:
        strategy_name = task.get("strategy")
        symbol = task.get("symbol")
        direction = task.get("direction")

        # 🔹 Найти стратегию
        strategy_id = None
        for sid, data in self.strategies_cache.items():
            if data["name"] == strategy_name:
                strategy_id = sid
                strategy = data
                break

        if strategy_id is None:
            return False, "Базовые проверки не пройдены — стратегия не найдена"

        # 🔹 Проверка разрешённого тикера
        allowed = self.strategy_allowed_tickers.get(strategy_id, set())
        if symbol not in allowed:
            return False, "Базовые проверки не пройдены — тикер не разрешён для этой стратегии"

        # 🔹 Поиск открытой позиции
        for pos in self.open_positions.values():
            if pos["strategy_id"] == strategy_id and pos["symbol"] == symbol:
                if pos["direction"] == direction:
                    return False, "Базовые проверки не пройдены — позиция в этом направлении уже открыта"
                else:
                    if not strategy.get("reverse", False):
                        return False, "Базовые проверки не пройдены — противоположная позиция уже открыта, реверс запрещён"
                    else:
                        return True, "Разрешён реверс — дальнейшие действия определяются стратегией"

        return True, "Базовые проверки пройдены"

    # 🔸 Поиск ID стратегии по имени
    async def get_strategy_id_by_name(self, strategy_name: str) -> int:
        for sid, data in self.strategies_cache.items():
            if data["name"] == strategy_name:
                return sid
        return None
    # 🔸 Получение значения индикатора из Redis по ключу
    async def get_indicator_value(self, symbol: str, timeframe: str, *path_parts: str) -> Decimal | None:
        try:
            key = f"{symbol}:{timeframe}:" + ":".join(path_parts)
            value = await self.redis.get(key)
            if value is None:
                logging.warning(f"⚠️ Индикатор не найден: {key}")
                return None
            return Decimal(value)
        except Exception as e:
            logging.error(f"❌ Ошибка при получении индикатора {key}: {e}")
            return None        