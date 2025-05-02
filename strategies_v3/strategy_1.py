# 🔸 Стратегия strategy_1

class Strategy1:
    def __init__(self):
        # 🔸 Инициализация стратегии без параметров
        pass

    async def on_signal(self, task: dict, interface):
        # 🔸 Метод обработки входящего сигнала
        strategy_name = task.get("strategy")
        symbol = task.get("symbol")
        direction = task.get("direction")
        log_id = task.get("log_id")

        print(f"⚙️ Strategy1: обработка сигнала {strategy_name} {symbol} {direction}, log_id={log_id}")