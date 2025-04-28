import asyncpg
import logging

class StrategyInterface:
    def __init__(self, database_url):
        self.database_url = database_url

    async def open_position(self, strategy_name, symbol, direction, params):
        logging.info(
            f"Попытка открыть позицию (стратегия: {strategy_name}, "
            f"тикер: {symbol}, направление: {direction}, параметры: {params})"
        )

    async def get_strategy_params(self, strategy_name):
        conn = await asyncpg.connect(self.database_url)
        try:
            query = """
            SELECT id, deposit, position_limit, use_all_tickers, timeframe
            FROM strategies
            WHERE name = $1 AND enabled = true
            """
            params = await conn.fetchrow(query, strategy_name)
            return params
        except Exception as e:
            logging.error(f"Ошибка загрузки параметров стратегии '{strategy_name}': {e}")
            return None
        finally:
            await conn.close()

    # Метод выполнения базовых проверок перед открытием позиции
    async def perform_basic_checks(self, strategy_params, symbol):
        conn = await asyncpg.connect(self.database_url)
        try:
            strategy_id = strategy_params['id']

            # Проверка №1: Размер открытых сделок не превышает депозит
            query_positions = """
            SELECT COALESCE(SUM(notional_value), 0) AS total_open_positions
            FROM positions
            WHERE strategy_id = $1 AND status = 'open'
            """
            total_open_positions = await conn.fetchval(query_positions, strategy_id)
            if total_open_positions >= strategy_params['deposit']:
                return False, "Депозит исчерпан открытыми позициями"

            # Проверка №2: Разрешена ли торговля по тикеру
            query_ticker = "SELECT tradepermission FROM tickers WHERE symbol = $1"
            ticker_tradepermission = await conn.fetchval(query_ticker, symbol)
            if not ticker_tradepermission:
                return False, "Торговля по тикеру запрещена (tradepermission=false)"

            # Проверка №3: Если стратегия ограничивает набор тикеров
            if not strategy_params['use_all_tickers']:
                query_strategy_ticker = """
                SELECT enabled FROM strategy_tickers
                WHERE strategy_id = $1 AND ticker_id = (SELECT id FROM tickers WHERE symbol = $2)
                """
                strategy_ticker_enabled = await conn.fetchval(query_strategy_ticker, strategy_id, symbol)
                if not strategy_ticker_enabled:
                    return False, "Тикер не разрешен для этой стратегии"

            # Все проверки пройдены
            return True, "Базовые проверки пройдены успешно"
        except Exception as e:
            logging.error(f"Ошибка выполнения базовых проверок: {e}")
            return False, f"Ошибка выполнения проверок: {e}"
        finally:
            await conn.close()            