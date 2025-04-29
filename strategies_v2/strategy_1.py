import logging
import os
import redis.asyncio as redis
from decimal import Decimal, ROUND_DOWN

class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, signal):
        logging.info(f"Стратегия №1 получила сигнал: {signal}")

        params = await self.load_params()
        if not params:
            message = "Ошибка загрузки параметров стратегии"
            logging.error(message)
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=None,
                status='ignored_by_check',
                note=message
            )
            return

        direction = signal['direction']

        # Базовые проверки должны быть ДО получения цены!
        checks_passed, message = await self.interface.perform_basic_checks(params, signal['symbol'], direction)
        if not checks_passed:
            logging.warning(f"Базовые проверки не пройдены: {message}")
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='ignored_by_check',
                note=message
            )
            return

        current_price = await self.get_current_price(signal['symbol'])
        if not current_price:
            message = f"Нет текущей цены для тикера {signal['symbol']}"
            logging.warning(message)
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='ignored_by_check',
                note=message
            )
            return

        checks_passed = await self.run_checks(params, signal, current_price)
        if not checks_passed:
            message = "Специфичные проверки не пройдены"
            logging.warning(message)
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='ignored_by_check',
                note=message
            )
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
        
    # Метод специфичных проверок (EMA50/ATR)
    async def run_checks(self, params, signal, current_price):
        ema, atr = await self.interface.get_ema_atr(signal['symbol'], params['timeframe'])

        if ema is None or atr is None:
            logging.warning(f"Недостаточно данных (EMA50 или ATR) для {signal['symbol']}.")
            return False

        if 'LONG' in signal['phrase']:
            condition = current_price >= (ema - Decimal('0.5') * atr)
            message = (
                f"Цена {current_price} ниже требуемой {(ema - Decimal('0.5') * atr)}. Вход отменён."
                if not condition else "Лонг вход разрешен."
            )
        elif 'SHORT' in signal['phrase']:
            condition = current_price <= (ema + Decimal('0.5') * atr)
            message = (
                f"Цена {current_price} выше требуемой {(ema + Decimal('0.5') * atr)}. Вход отменён."
                if not condition else "Шорт вход разрешен."
            )
        else:
            logging.warning("Неизвестное направление сигнала.")
            return False

        if not condition:
            logging.warning(message)
            return False

        logging.info("Специфичные проверки пройдены успешно.")
        return True
        
    # Метод открытия виртуальной позиции (с учётом комиссий)
    async def open_position(self, params, signal, current_price):
        direction = signal['direction']
        position_size = await self.interface.calculate_position_size(params, signal['symbol'], current_price)

        if not position_size:
            logging.warning("Ошибка расчёта размера позиции.")
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='ignored_by_check',
                note='Ошибка расчёта размера позиции'
            )
            return

        logging.info(
            f"Расчётный размер позиции для {signal['symbol']}: {position_size} по цене {current_price} USDT "
            f"(общая сумма {Decimal(position_size) * Decimal(current_price)} USDT)"
        )

        position_id = await self.interface.open_virtual_position(
            strategy_id=params['id'],
            log_id=signal['log_id'],
            symbol=signal['symbol'],
            direction=direction,
            entry_price=current_price,
            quantity=position_size
        )

        if position_id:
            logging.info(f"Позиция успешно открыта с ID={position_id}")

            # Логируем успешное открытие позиции
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='position_opened',
                position_id=position_id,
                note=f"Позиция открыта: {signal['symbol']}, направление: {direction}, qty={position_size}, цена={current_price}"
            )

            ema, atr = await self.interface.get_ema_atr(signal['symbol'], params['timeframe'])
            if not atr:
                logging.error("Не удалось получить ATR для расчёта TP/SL.")
                return

            targets = await self.calculate_tp_sl(
                symbol=signal['symbol'],
                direction=direction,
                entry_price=current_price,
                quantity=position_size,
                atr=atr
            )

            await self.interface.create_position_targets(position_id, targets)
        else:
            logging.error("Ошибка открытия позиции!")
            await self.interface.log_strategy_action(
                log_id=signal['log_id'],
                strategy_id=params['id'],
                status='ignored_by_check',
                note='Ошибка открытия позиции'
            )
    # Метод расчёта уровней TP и SL для позиции (стратегия №1)
    async def calculate_tp_sl(self, symbol, direction, entry_price, quantity, atr):
        precision = await self.interface.get_precision_price(symbol)
        precision_format = Decimal(f'1e-{precision}')

        tp_levels = [
            {"level": 1, "multiplier": Decimal('1.5'), "quantity_pct": Decimal('0.5')},
            {"level": 2, "multiplier": Decimal('2.5'), "quantity_pct": Decimal('0.3')},
            {"level": 3, "multiplier": Decimal('3.5'), "quantity_pct": Decimal('0.2')},
        ]

        targets = []

        # TP
        for tp in tp_levels:
            tp_price = (entry_price + tp['multiplier'] * atr) if direction == 'long' else (entry_price - tp['multiplier'] * atr)
            tp_price = tp_price.quantize(precision_format, rounding=ROUND_DOWN)
            tp_quantity = (quantity * tp['quantity_pct']).quantize(Decimal('1e-8'))
            targets.append({
                "type": "TP",
                "price": tp_price,
                "quantity": tp_quantity,
                "level": tp['level']
            })

        # SL (100% на 1.5 ATR)
        sl_price = (entry_price - Decimal('1.5') * atr) if direction == 'long' else (entry_price + Decimal('1.5') * atr)
        sl_price = sl_price.quantize(precision_format, rounding=ROUND_DOWN)
        targets.append({
            "type": "SL",
            "price": sl_price,
            "quantity": quantity,
            "level": None
        })

        return targets