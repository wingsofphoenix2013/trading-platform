import logging

# Инициализация стратегии №1
class Strategy1:
    def __init__(self):
        logging.info("Стратегия №1 запустилась и готова принимать сигналы.")

    async def on_signal(self, signal):
        # Заглушка приёма сигнала
        logging.info(f"Стратегия №1 получила сигнал: {signal}")