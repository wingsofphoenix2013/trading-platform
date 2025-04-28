import logging

class StrategyInterface:
    def __init__(self):
        pass

    async def open_position(self, strategy_name, symbol, direction, params):
        logging.info(
            f"Попытка открыть позицию (стратегия: {strategy_name}, "
            f"тикер: {symbol}, направление: {direction}, параметры: {params})"
        )