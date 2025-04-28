import logging

class Strategy1:
    def __init__(self, interface):
        self.interface = interface

    async def on_signal(self, signal):
        logging.info(f"Стратегия №1 получила сигнал: {signal}")
        await self.interface.open_position(
            strategy_name='strategy_1',
            symbol=signal['symbol'],
            direction='long' if 'LONG' in signal['phrase'] else 'short',
            params={}
        )