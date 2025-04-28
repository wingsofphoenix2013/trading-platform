import logging

class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, signal):
        logging.info(f"Стратегия №1 получила сигнал: {signal}")

        # Загружаем параметры из базы
        params = await self.interface.get_strategy_params('test-1')
        if params:
            logging.info(f"Параметры стратегии загружены: {params}")

            # Выполняем базовые проверки
            checks_passed, message = await self.interface.perform_basic_checks(params, signal['symbol'])
            if checks_passed:
                logging.info(f"Базовые проверки пройдены: {message}")
            else:
                logging.warning(f"Базовые проверки НЕ пройдены: {message}")
        else:
            logging.error("Не удалось загрузить параметры стратегии.")