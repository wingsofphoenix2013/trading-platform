import logging

class Strategy1:
    def __init__(self):
        pass

    async def on_signal(self, task: dict, interface):
        logging.info("📈 Продолжение логики внутри strategy_1...")

        # 🔹 Расчёт параметров позиции
        result = await interface.calculate_position_size(task)

        if result is None:
            logging.warning("⚠️ Расчёт позиции завершён без результата — позиция не будет открыта")
            return

        logging.info(f"📊 Расчёт позиции (strategy_1): "
                     f"qty={result['quantity']}, notional={result['notional_value']}, "
                     f"risk={result['planned_risk']}, margin={result['margin_used']}, "
                     f"sl={result['stop_loss_price']}")