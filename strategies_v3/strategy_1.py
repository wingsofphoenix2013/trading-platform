import logging

class Strategy1:
    def __init__(self):
        pass

    async def on_signal(self, task: dict, interface):
        logging.info("📈 Продолжение логики внутри strategy_1...")
        # Здесь будет бизнес-логика открытия позиции и т.д.