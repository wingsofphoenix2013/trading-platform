import logging

class Strategy1:
    def __init__(self):
        pass

    async def on_signal(self, task: dict, interface):
        logging.info("📈 Продолжение логики внутри strategy_1...")

        # 🔸 Временно пропущены специфические проверки
        # 🔹 Пытаемся открыть позицию
        position_id = await interface.open_position(task)

        if position_id:
            logging.info(f"✅ Стратегия открыла позицию ID={position_id}")
        else:
            logging.warning("⚠️ Стратегия не открыла позицию")