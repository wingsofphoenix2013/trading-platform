import logging
import os
import redis.asyncio as redis
from decimal import Decimal

class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, signal):
        logging.info(f"Стратегия №1 получила сигнал: {signal}")

        params = await self.interface.get_strategy_params('test-1')
        if not params:
            logging.error("Не удалось загрузить параметры стратегии.")
            return

        logging.info(f"Параметры стратегии загружены: {params}")

        # Выполняем базовые проверки
        checks_passed, message = await self.interface.perform_basic_checks(params, signal['symbol'])
        if not checks_passed:
            logging.warning(f"Базовые проверки НЕ пройдены: {message}")
            return

        logging.info(f"Базовые проверки пройдены: {message}")

        # Получаем EMA50 и ATR
        ema50, atr = await self.interface.get_ema_atr(signal['symbol'], params['timeframe'])
        if ema50 is None or atr is None:
            logging.warning("Не удалось получить EMA50 и ATR, вход отменён.")
            return

        # Получаем текущую цену
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            ssl=True
        )
        current_markprice = await redis_client.get(f"{signal['symbol']}:markprice")
        await redis_client.close()

        if current_markprice is None:
            logging.warning("Нет текущей цены, вход отменён.")
            return

        current_markprice = Decimal(current_markprice)

        # Проверка условий входа
        if 'LONG' in signal['phrase']:
            required_price = ema50 - Decimal('0.5') * atr
            if current_markprice < required_price:
                logging.warning(f"Цена {current_markprice} ниже требуемой {required_price}. Вход отменён.")
                return
        elif 'SHORT' in signal['phrase']:
            required_price = ema50 + Decimal('0.5') * atr
            if current_markprice > required_price:
                logging.warning(f"Цена {current_markprice} выше требуемой {required_price}. Вход отменён.")
                return

        logging.info("Специфичные проверки пройдены, открываем позицию.")
        await self.interface.open_position(
            strategy_name='strategy_1',
            symbol=signal['symbol'],
            direction='long' if 'LONG' in signal['phrase'] else 'short',
            params={}
        )