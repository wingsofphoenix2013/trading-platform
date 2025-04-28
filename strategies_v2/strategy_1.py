import logging
import os
import redis.asyncio as redis
from decimal import Decimal

class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, signal):
        logging.info(f"Стратегия №1 получила сигнал: {signal}")

        params = await self.load_params()
        if not params:
            return

        current_price = await self.get_current_price(signal['symbol'])
        if not current_price:
            return

        checks_passed = await self.run_checks(params, signal, current_price)
        if not checks_passed:
            return

        await self.open_position(params, signal, current_price)

    # Метод загрузки параметров стратегии
    async def load_params(self):
        params = await self.interface.get_strategy_params('test-1')
        if not params:
            logging.error("Не удалось загрузить параметры стратегии.")
        else:
            logging.info(f"Параметры стратегии загружены: {params}")
        return params

    # Метод получения текущей цены из Redis
    async def get_current_price(self, symbol):
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            ssl=True
        )
        price_str = await redis_client.get(f"price:{symbol}")
        await redis_client.close()

        if not price_str:
            logging.warning(f"Нет текущей цены для тикера {symbol}. Пропускаем.")
            return None

        try:
            price = Decimal(price_str)
            return price
        except Exception as e:
            logging.error(f"Ошибка преобразования цены из Redis в Decimal для тикера {symbol}: {e}")
            return None
        
    # Метод специфичных проверок (например, EMA50/ATR)
    async def run_checks(self, params, signal, current_price):
        result = await self.interface.check_ema_atr(
            symbol=signal['symbol'],
            timeframe=params['timeframe'],
            markprice=current_price,
            direction=signal['phrase']
        )
        if not result['can_trade']:
            logging.warning(result['message'])
            return False

        logging.info("Специфичные проверки пройдены, открываем позицию.")
        return True

    # Метод открытия виртуальной позиции (с учётом комиссий)
    async def open_position(self, params, signal, current_price):
        position_size = await self.interface.calculate_position_size(params, signal['symbol'], current_price)

        if not position_size:
            logging.warning("Ошибка расчёта размера позиции.")
            return

        logging.info(
            f"Расчётный размер позиции для {signal['symbol']}: {position_size} по цене {current_price} USDT "
            f"(общая сумма {Decimal(position_size) * Decimal(current_price)} USDT)"
        )

        position_id = await self.interface.open_virtual_position(
            strategy_id=params['id'],
            log_id=signal['log_id'],
            symbol=signal['symbol'],
            direction='long' if 'LONG' in signal['phrase'] else 'short',
            entry_price=current_price,
            quantity=position_size
        )

        if position_id:
            logging.info(f"Позиция успешно открыта с ID={position_id}")
        else:
            logging.error("Ошибка открытия позиции!")