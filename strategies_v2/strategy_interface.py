import asyncpg
import logging
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN
import os

class StrategyInterface:
    def __init__(self, database_url):
        self.database_url = database_url

    async def open_position(self, strategy_name, symbol, direction, params):
        logging.info(
            f"Попытка открыть позицию (стратегия: {strategy_name}, "
            f"тикер: {symbol}, направление: {direction}, параметры: {params})"
        )
    # Загрузка параметров стратегии
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
    # Метод расчёта текущей загрузки депозита
    async def calculate_current_deposit_usage(self, strategy_id):
        conn = await asyncpg.connect(self.database_url)
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            ssl=True
        )
        try:
            positions = await conn.fetch(
                """
                SELECT p.symbol, p.quantity_left, t.precision_price, t.precision_qty
                FROM positions p
                JOIN tickers t ON p.symbol = t.symbol
                WHERE p.strategy_id = $1 AND p.status = 'open'
                """,
                strategy_id
            )

            total_usage = Decimal('0')
            for pos in positions:
                symbol = pos['symbol']
                quantity_left = Decimal(pos['quantity_left'])
                precision_qty = pos['precision_qty']
                precision_price = pos['precision_price']

                # Берём текущую цену из Redis
                current_markprice = await redis_client.get(f"{symbol}:markprice")
                if current_markprice is None:
                    logging.warning(f"Нет текущей цены для тикера {symbol}. Пропускаем.")
                    continue

                current_markprice = Decimal(current_markprice)

                # Округляем quantity_left и текущую цену
                quantity_left = quantity_left.quantize(Decimal(f'1e-{precision_qty}'), rounding=ROUND_DOWN)
                current_markprice = current_markprice.quantize(Decimal(f'1e-{precision_price}'), rounding=ROUND_DOWN)

                # Вычисляем текущий notional_value позиции
                position_notional_value = (quantity_left * current_markprice).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)

                total_usage += position_notional_value

            return total_usage

        except Exception as e:
            logging.error(f"Ошибка при расчёте текущей загрузки депозита: {e}")
            return None
        finally:
            await conn.close()
            await redis_client.close()
    # Метод выполнения базовых проверок перед открытием позиции
    async def perform_basic_checks(self, strategy_params, symbol):
        conn = await asyncpg.connect(self.database_url)
        try:
            strategy_id = strategy_params['id']

            # Проверка №1: Размер открытых сделок не превышает депозит
            total_open_positions = await self.calculate_current_deposit_usage(strategy_id)
            if total_open_positions is None:
                return False, "Ошибка при расчёте текущей загрузки депозита"

            if total_open_positions >= strategy_params['deposit']:
                return False, "Депозит исчерпан текущими позициями"

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
    # Метод получения EMA50 и ATR из Redis
    async def get_ema_atr(self, symbol, timeframe):
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            ssl=True
        )
        try:
            ema_key = f"{symbol}:{timeframe}:EMA:50"
            atr_key = f"{symbol}:{timeframe}:ATR:atr"

            ema_value = await redis_client.get(ema_key)
            atr_value = await redis_client.get(atr_key)

            if ema_value is None or atr_value is None:
                logging.warning(f"Не найдены данные EMA или ATR для {symbol} {timeframe}")
                return None, None

            return Decimal(ema_value), Decimal(atr_value)

        except Exception as e:
            logging.error(f"Ошибка получения EMA и ATR: {e}")
            return None, None
        finally:
            await redis_client.close()                        