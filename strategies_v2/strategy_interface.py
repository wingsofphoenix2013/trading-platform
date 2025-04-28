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
    async def perform_basic_checks(self, strategy_params, symbol, direction):
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
            query_ticker = """
            SELECT tradepermission FROM tickers WHERE symbol = $1
            """
            ticker_tradepermission = await conn.fetchval(query_ticker, symbol)

            if ticker_tradepermission != 'enabled':
                return False, "Торговля по тикеру запрещена (tradepermission != 'enabled')"

            # Проверка №3: Если стратегия ограничивает набор тикеров
            if not strategy_params['use_all_tickers']:
                query_strategy_ticker = """
                SELECT enabled FROM strategy_tickers
                WHERE strategy_id = $1 AND ticker_id = (SELECT id FROM tickers WHERE symbol = $2)
                """
                strategy_ticker_enabled = await conn.fetchval(query_strategy_ticker, strategy_id, symbol)
                if not strategy_ticker_enabled:
                    return False, "Тикер не разрешен для этой стратегии"

            # Проверка №4: Проверка наличия открытых позиций по тикеру и направлению
            already_open = await self.has_open_position(strategy_id, symbol, direction)
            if already_open:
                return False, "Уже есть открытая позиция по этому тикеру и направлению"

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
    # Метод расчёта размера позиции с контролем итогового значения
    async def calculate_position_size(self, strategy_params, symbol, price):
        conn = await asyncpg.connect(self.database_url)
        try:
            # Получаем precision_qty из таблицы tickers
            precision_qty = await conn.fetchval("SELECT precision_qty FROM tickers WHERE symbol = $1", symbol)

            if precision_qty is None:
                logging.error(f"Не найдена точность (precision_qty) для тикера {symbol}")
                return None
            
            # Максимальный объём позиции
            max_qty = Decimal(strategy_params['position_limit']) / Decimal(price)
            position_qty_step = Decimal(f'1e-{precision_qty}')
            
            # Округляем вниз, чтобы гарантированно не превышать лимит
            position_qty = max_qty.quantize(position_qty_step)

            # Проверка итогового значения, уменьшаем до попадания в лимит
            while (position_qty * Decimal(price)) > Decimal(strategy_params['position_limit']):
                position_qty -= position_qty_step

            # Проверка минимально допустимого размера (90%)
            min_allowed_qty = (Decimal('0.9') * max_qty).quantize(position_qty_step)
            if position_qty < min_allowed_qty:
                logging.warning("Расчётный объём меньше минимально разрешённого (90%).")
                return None

            return position_qty

        except Exception as e:
            logging.error(f"Ошибка расчёта размера позиции: {e}")
            return None
        finally:
            await conn.close()
    # Метод создания виртуальной позиции в базе данных
    async def open_virtual_position(self, strategy_id, log_id, symbol, direction, entry_price, quantity):
        conn = await asyncpg.connect(self.database_url)
        try:
            notional_value = (Decimal(entry_price) * Decimal(quantity)).quantize(Decimal('1e-8'))

            # Начальный PnL с учетом комиссии 0.05%
            commission = (notional_value * Decimal('0.0005')).quantize(Decimal('1e-8'))
            initial_pnl = -commission

            query = """
            INSERT INTO positions
            (strategy_id, log_id, symbol, direction, entry_price, quantity, notional_value, quantity_left, status, created_at, pnl)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $6, 'open', NOW(), $8)
            RETURNING id
            """
            position_id = await conn.fetchval(query, strategy_id, log_id, symbol, direction, entry_price, quantity, notional_value, initial_pnl)
            
            logging.info(f"Открыта позиция ID={position_id}, тикер={symbol}, направление={direction}, размер={quantity}, цена входа={entry_price}, комиссия={commission}")
            
            return position_id

        except Exception as e:
            logging.error(f"Ошибка при открытии позиции: {e}")
            return None
        finally:
            await conn.close()
    # Метод добавления уровней TP и SL в таблицу position_targets
    async def create_position_targets(self, position_id, targets):
        """
        Создаёт записи TP и SL для указанной позиции.

        :param position_id: ID открытой позиции
        :param targets: список словарей с уровнями TP и SL в формате:
            [
                {"type": "TP", "price": Decimal, "quantity": Decimal, "level": 1},
                {"type": "SL", "price": Decimal, "quantity": Decimal, "level": None}
            ]
        """
        conn = await asyncpg.connect(self.database_url)
        try:
            query = """
            INSERT INTO position_targets 
                (position_id, type, price, quantity, level, hit, canceled)
            VALUES ($1, $2, $3, $4, $5, false, false)
            """
            for target in targets:
                await conn.execute(
                    query,
                    position_id,
                    target["type"].lower(),
                    target["price"],
                    target["quantity"],
                    target["level"]
                )
            logging.info(f"Добавлены уровни TP/SL для позиции ID={position_id}")
        except Exception as e:
            logging.error(f"Ошибка создания уровней TP/SL: {e}")
        finally:
            await conn.close()
    # Метод проверки наличия открытых позиций по стратегии, тикеру и направлению
    async def has_open_position(self, strategy_id, symbol, direction):
        conn = await asyncpg.connect(self.database_url)
        try:
            query = """
            SELECT COUNT(*) FROM positions
            WHERE strategy_id = $1 AND symbol = $2 AND direction = $3 
              AND status IN ('open', 'partial')
            """
            count = await conn.fetchval(query, strategy_id, symbol, direction)
            return count > 0
        except Exception as e:
            logging.error(f"Ошибка проверки открытых позиций: {e}")
            return True  # При ошибке считаем, что позиция есть (для безопасности)
        finally:
            await conn.close()                      